"""
IAM Service Emulator (AWS-compatible).

STS actions are in sts.py.

IAM actions:
  CreateUser, GetUser, ListUsers, DeleteUser,
  CreateRole, GetRole, ListRoles, DeleteRole,
  CreatePolicy, GetPolicy, GetPolicyVersion, ListPolicyVersions, ListPolicies, DeletePolicy,
  CreatePolicyVersion, DeletePolicyVersion,
  AttachRolePolicy, DetachRolePolicy, ListAttachedRolePolicies,
  PutRolePolicy, GetRolePolicy, DeleteRolePolicy, ListRolePolicies,
  AttachUserPolicy, DetachUserPolicy, ListAttachedUserPolicies,
  PutUserPolicy, GetUserPolicy, DeleteUserPolicy, ListUserPolicies,
  CreateAccessKey, ListAccessKeys, DeleteAccessKey, UpdateAccessKey, GetAccessKeyLastUsed,
  CreateInstanceProfile, DeleteInstanceProfile, GetInstanceProfile,
  AddRoleToInstanceProfile, RemoveRoleFromInstanceProfile,
  ListInstanceProfiles, ListInstanceProfilesForRole,
  UpdateAssumeRolePolicy,
  CreateGroup, GetGroup, DeleteGroup, ListGroups,
  AddUserToGroup, RemoveUserFromGroup, ListGroupsForUser,
  CreateServiceLinkedRole, DeleteServiceLinkedRole, GetServiceLinkedRoleDeletionStatus,
  CreateOpenIDConnectProvider, GetOpenIDConnectProvider, DeleteOpenIDConnectProvider,
  TagRole, UntagRole, ListRoleTags,
  TagUser, UntagUser, ListUserTags,
  TagPolicy, UntagPolicy, ListPolicyTags,
  SimulatePrincipalPolicy, SimulateCustomPolicy.
"""

import base64
import copy
import json
import logging
import os
import time
from urllib.parse import parse_qs
from urllib.parse import quote as _url_quote
from xml.sax.saxutils import escape as _xml_escape

from ministack.core.responses import AccountScopedDict, get_account_id, get_region, json_response, new_uuid

logger = logging.getLogger("iam")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------
from ministack.core.persistence import PERSIST_STATE, load_state

_users = AccountScopedDict()
_roles = AccountScopedDict()
_policies = AccountScopedDict()
_access_keys = AccountScopedDict()
_instance_profiles = AccountScopedDict()
_groups = AccountScopedDict()
_user_inline_policies = AccountScopedDict()
_oidc_providers = AccountScopedDict()
_service_linked_role_deletion_tasks = AccountScopedDict()
_sla_jobs = AccountScopedDict()
_saml_providers = AccountScopedDict()
_mfa_devices = AccountScopedDict()
_login_profiles = AccountScopedDict()
_credential_reports = AccountScopedDict()
_account_password_policy = AccountScopedDict()
_account_aliases = AccountScopedDict()


# -- AWS-managed policies ---------------------------------------------------
#
# Real AWS hosts AWS-managed policies under a virtual `aws` account
# (ARN form ``arn:aws:iam::aws:policy/<Name>``). Every customer can read
# them regardless of their own account. Ministack stores customer
# resources in ``AccountScopedDict``, which keys by the caller's account
# — so a 12-digit session can never read entries under the literal
# ``aws`` account. That breaks the common Terraform pattern:
#
#     data "aws_iam_policy" "admin" {
#       arn = "arn:aws:iam::aws:policy/AdministratorAccess"
#     }
#
# We model AWS-managed policies as a *separate*, non-account-scoped
# dict that every session reads from. They are immutable from a session
# perspective: CreatePolicy/DeletePolicy/Tag* on an AWS-managed ARN
# return AWS-parity errors (real AWS does not let customers mutate
# them). Attach/Detach work against any role or user — only the
# AttachmentCount counter is a no-op for AWS-managed policies.
_AWS_MANAGED_POLICY_PREFIX = "arn:aws:iam::aws:policy/"

# Maps full ARN -> policy record (same shape as customer-managed
# entries in ``_policies``). Populated at import time by
# ``_seed_aws_managed_policies``. Unknown AWS-managed ARNs return
# ``NoSuchEntity`` by default so that typos surface locally the same
# way they do against real AWS (e.g. ``AdminstratorAccess``). Opt in
# to permissive autocreate by setting
# ``MINISTACK_AUTOCREATE_AWS_MANAGED=1`` — useful when running
# Terraform locally against stacks that reference less common
# AWS-managed policies.
_aws_managed_policies: dict = {}

# AttachmentCount for AWS-managed policies is per-(session-account, arn):
# real AWS reports the count of *the calling account's* roles + users
# attached to the policy, not a global counter. We can't store the
# count on the shared ``_aws_managed_policies`` record (it would leak
# across sessions), so a sidecar AccountScopedDict holds the
# per-account totals. Reads merge this into the record on the way out
# in ``_managed_policy_xml``.
_aws_managed_attachment_counts = AccountScopedDict()


def _bump_aws_managed_attachment(arn: str, delta: int) -> None:
    current = _aws_managed_attachment_counts.get(arn, 0)
    _aws_managed_attachment_counts[arn] = max(current + delta, 0)


def _is_aws_managed_arn(arn: str) -> bool:
    return isinstance(arn, str) and arn.startswith(_AWS_MANAGED_POLICY_PREFIX)


def _autocreate_aws_managed_enabled() -> bool:
    return os.environ.get("MINISTACK_AUTOCREATE_AWS_MANAGED", "0").lower() in (
        "1", "true", "yes",
    )


def _autovivify_aws_managed_policy(arn: str) -> dict:
    """Lazily create a permissive AWS-managed policy record for ``arn``.

    Real AWS publishes ~1k AWS-managed policies; we pre-seed the most
    commonly referenced ones (see ``_seed_aws_managed_policies``) and
    fall back to this on first GetPolicy for anything else. The
    fallback document allows every action so terraform plans that
    attach the policy to a role still produce a sensible diff.
    """
    name = arn[len(_AWS_MANAGED_POLICY_PREFIX):]
    if not name:
        return None  # type: ignore[return-value]
    doc = json.dumps({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "*", "Resource": "*"}],
    })
    record = _make_aws_managed_record(name, doc, description=f"AWS-managed policy {name} (autocreated by Ministack).")
    _aws_managed_policies[arn] = record
    return record


def _make_aws_managed_record(name: str, document: str, description: str = "") -> dict:
    """Build a policy record matching the shape ``_create_policy`` stores."""
    arn = f"{_AWS_MANAGED_POLICY_PREFIX}{name}"
    created = _now()
    return {
        "PolicyName": name,
        "Arn": arn,
        "PolicyId": _gen_id("ANPA"),
        "CreateDate": created,
        "UpdateDate": created,
        "DefaultVersionId": "v1",
        "AttachmentCount": 0,
        "IsAttachable": True,
        "Path": "/",
        "Description": description,
        "Tags": [],
        "Versions": {
            "v1": {
                "Document": document,
                "VersionId": "v1",
                "IsDefaultVersion": True,
                "CreateDate": created,
            }
        },
    }


def _lookup_policy(arn: str):
    """Return the policy record for ``arn`` from either the
    account-scoped customer-managed store or the global AWS-managed
    store, autoviving on the AWS-managed side if enabled."""
    if _is_aws_managed_arn(arn):
        record = _aws_managed_policies.get(arn)
        if record is None and _autocreate_aws_managed_enabled():
            record = _autovivify_aws_managed_policy(arn)
        return record
    return _policies.get(arn)


def _policy_exists(arn: str) -> bool:
    return _lookup_policy(arn) is not None


def _seed_aws_managed_policies() -> None:
    """Seed the global AWS-managed policy store with the canonical
    documents for the most commonly referenced AWS-managed policies.
    Documents and descriptions are mirrored verbatim from the AWS
    Managed Policy Reference
    (https://docs.aws.amazon.com/aws-managed-policy/latest/reference/);
    update via ``aws iam get-policy-version`` against real AWS when
    a new policy version ships."""
    seeds = [
        ('AdministratorAccess',
         '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}',
         "Provides full access to AWS services and resources."),
        ('PowerUserAccess',
         '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","NotAction":["iam:*","organizations:*","account:*"],"Resource":"*"},{"Effect":"Allow","Action":["account:GetAccountInformation","account:GetGovCloudAccountInformation","account:GetPrimaryEmail","account:ListRegions","iam:CreateServiceLinkedRole","iam:DeleteServiceLinkedRole","iam:ListRoles","organizations:DescribeEffectivePolicy","organizations:DescribeOrganization"],"Resource":"*"}]}',
         "Provides full access to AWS services and resources, but does not allow management of Users and groups."),
        ('ReadOnlyAccess',
         '{"Version":"2012-10-17","Statement":[{"Sid":"ReadOnlyActionsGroup1","Effect":"Allow","Action":["a4b:Get*","a4b:List*","a4b:Search*","access-analyzer:GetAccessPreview","access-analyzer:GetAnalyzedResource","access-analyzer:GetAnalyzer","access-analyzer:GetArchiveRule","access-analyzer:GetFinding","access-analyzer:GetFindingsStatistics","access-analyzer:GetGeneratedPolicy","access-analyzer:ListAccessPreviewFindings","access-analyzer:ListAccessPreviews","access-analyzer:ListAnalyzedResources","access-analyzer:ListAnalyzers","access-analyzer:ListArchiveRules","access-analyzer:ListFindings","access-analyzer:ListPolicyGenerations","access-analyzer:ListTagsForResource","access-analyzer:ValidatePolicy","account:GetAccountInformation","account:GetAlternateContact","account:GetContactInformation","account:GetGovCloudAccountInformation","account:GetPrimaryEmail","account:GetRegionOptStatus","account:ListRegions","acm-pca:Describe*","acm-pca:Get*","acm-pca:List*","acm:Describe*","acm:Get*","acm:List*","acm:SearchCertificates","action-recommendations:ListRecommendedActions","aiops:GetEphemeralInvestigationResults","aiops:GetFact","aiops:GetFactVersions","aiops:GetInvestigation","aiops:GetInvestigationEvent","aiops:GetInvestigationGroup","aiops:GetInvestigationResource","aiops:GetReport","aiops:ListFacts","aiops:ListInvestigationEvents","aiops:ListInvestigationGroups","aiops:ListInvestigations","aiops:ValidateInvestigationGroup","airflow:ListEnvironments","airflow:ListTagsForResource","amplify:GetApp","amplify:GetBackendEnvironment","amplify:GetBranch","amplify:GetDomainAssociation","amplify:GetJob","amplify:GetWebhook","amplify:ListApps","amplify:ListArtifacts","amplify:ListBackendEnvironments","amplify:ListBranches","amplify:ListDomainAssociations","amplify:ListJobs","amplify:ListTagsForResource","amplify:ListWebhooks","aoss:BatchGetCollection","aoss:BatchGetCollectionGroup","aoss:BatchGetLifecyclePolicy","aoss:BatchGetVpcEndpoint","aoss:GetAccessPolicy","aoss:GetAccountSettings","aoss:GetPoliciesStats","aoss:GetSecurityConfig","aoss:GetSecurityPolicy","aoss:ListAccessPolicies","aoss:ListCollectionGroups","aoss:ListCollections","aoss:ListLifecyclePolicies","aoss:ListSecurityConfigs","aoss:ListSecurityPolicies","aoss:ListTagsForResource","aoss:ListVpcEndpoints","apigateway:GET","apigateway:GetPortal","apigateway:GetPortalProduct","apigateway:GetProductPage","apigateway:GetProductRestEndpointPage","apigateway:GetRoutingRule","apigateway:ListPortalProducts","apigateway:ListPortals","apigateway:ListProductPages","apigateway:ListProductRestEndpointPages","apigateway:ListRoutingRules","appconfig:GetApplication","appconfig:GetConfiguration","appconfig:GetConfigurationProfile","appconfig:GetDeployment","appconfig:GetDeploymentStrategy","appconfig:GetEnvironment","appconfig:GetExtension","appconfig:GetHostedConfigurationVersion","appconfig:ListApplications","appconfig:ListConfigurationProfiles","appconfig:ListDeployments","appconfig:ListDeploymentStrategies","appconfig:ListEnvironments","appconfig:ListExtensions","appconfig:ListHostedConfigurationVersions","appconfig:ListTagsForResource","appfabric:GetAppAuthorization","appfabric:GetAppBundle","appfabric:GetIngestion","appfabric:GetIngestionDestination","appfabric:ListAppAuthorizations","appfabric:ListAppBundles","appfabric:ListIngestionDestinations","appfabric:ListIngestions","appfabric:ListTagsForResource","appflow:DescribeConnector","appflow:DescribeConnectorEntity","appflow:DescribeConnectorFields","appflow:DescribeConnectorProfiles","appflow:DescribeConnectors","appflow:DescribeFlow","appflow:DescribeFlowExecution","appflow:DescribeFlowExecutionRecords","appflow:DescribeFlows","appflow:ListConnectorEntities","appflow:ListConnectorFields","appflow:ListConnectors","appflow:ListFlows","appflow:ListTagsForResource","application-autoscaling:Describe*","application-autoscaling:GetPredictiveScalingForecast","application-autoscaling:ListTagsForResource","application-signals:BatchGetServiceLevelObjectiveBudgetReport","application-signals:GetService","application-signals:GetServiceLevelObjective","application-signals:ListAuditFindings","application-signals:ListEntityEvents","application-signals:ListGroupingAttributeDefinitions","application-signals:ListObservedEntities","application-signals:ListServiceDependencies","application-signals:ListServiceDependents","application-signals:ListServiceLevelObjectiveExclusionWindows","application-signals:ListServiceLevelObjectives","application-signals:ListServiceOperations","application-signals:ListServices","application-signals:ListServiceStates","application-signals:ListTagsForResource","applicationinsights:Describe*","applicationinsights:List*","appmesh:Describe*","appmesh:List*","apprunner:DescribeAutoScalingConfiguration","apprunner:DescribeCustomDomains","apprunner:DescribeObservabilityConfiguration","apprunner:DescribeService","apprunner:DescribeVpcConnector","apprunner:DescribeVpcIngressConnection","apprunner:DescribeWebAclForService","apprunner:ListAssociatedServicesForWebAcl","apprunner:ListAutoScalingConfigurations","apprunner:ListConnections","apprunner:ListObservabilityConfigurations","apprunner:ListOperations","apprunner:ListServices","apprunner:ListServicesForAutoScalingConfiguration","apprunner:ListTagsForResource","apprunner:ListVpcConnectors","apprunner:ListVpcIngressConnections","appstream:Describe*","appstream:List*","appstudio:GetAccountStatus","appstudio:GetEnablementJobStatus","appsync:Get*","appsync:List*","apptest:GetTestCase","apptest:GetTestConfiguration","apptest:GetTestRunStep","apptest:GetTestSuite","apptest:ListTagsForResource","apptest:ListTestCases","apptest:ListTestConfigurations","apptest:ListTestRuns","apptest:ListTestRunSteps","apptest:ListTestRunTestCases","apptest:ListTestSuites","aps:DescribeAlertManagerDefinition","aps:DescribeAnomalyDetector","aps:DescribeLoggingConfiguration","aps:DescribeQueryLoggingConfiguration","aps:DescribeResourcePolicy","aps:DescribeRuleGroupsNamespace","aps:DescribeScraper","aps:DescribeScraperLoggingConfiguration","aps:DescribeWorkspace","aps:DescribeWorkspaceConfiguration","aps:GetAlertManagerSilence","aps:GetAlertManagerStatus","aps:GetDefaultScraperConfiguration","aps:GetLabels","aps:GetMetricMetadata","aps:GetSeries","aps:ListAlertManagerAlertGroups","aps:ListAlertManagerAlerts","aps:ListAlertManagerReceivers","aps:ListAlertManagerSilences","aps:ListAlerts","aps:ListAnomalyDetectors","aps:ListRuleGroupsNamespaces","aps:ListRules","aps:ListScrapers","aps:ListTagsForResource","aps:ListWorkspaces","aps:PreviewAnomalyDetector","aps:QueryMetrics","arc-region-switch:GetPlan","arc-region-switch:GetPlanEvaluationStatus","arc-region-switch:GetPlanExecution","arc-region-switch:GetPlanInRegion","arc-region-switch:ListPlanExecutionEvents","arc-region-switch:ListPlanExecutions","arc-region-switch:ListPlans","arc-region-switch:ListPlansInRegion","arc-region-switch:ListRoute53HealthChecks","arc-region-switch:ListRoute53HealthChecksInRegion","arc-region-switch:ListTagsForResource","arc-zonal-shift:GetAutoshiftObserverNotificationStatus","arc-zonal-shift:GetManagedResource","arc-zonal-shift:ListAutoshifts","arc-zonal-shift:ListManagedResources","arc-zonal-shift:ListZonalShifts","artifact:GetCustomerAgreement","artifact:GetReport","artifact:GetReportMetadata","artifact:GetTermForReport","artifact:ListAgreements","artifact:ListCustomerAgreements","artifact:ListReports","artifact:ListReportVersions","athena:Batch*","athena:Get*","athena:List*","auditmanager:GetAccountStatus","auditmanager:GetAssessment","auditmanager:GetAssessmentFramework","auditmanager:GetAssessmentReportUrl","auditmanager:GetChangeLogs","auditmanager:GetControl","auditmanager:GetDelegations","auditmanager:GetEvidence","auditmanager:GetEvidenceByEvidenceFolder","auditmanager:GetEvidenceFolder","auditmanager:GetEvidenceFoldersByAssessment","auditmanager:GetEvidenceFoldersByAssessmentControl","auditmanager:GetOrganizationAdminAccount","auditmanager:GetServicesInScope","auditmanager:GetSettings","auditmanager:ListAssessmentFrameworks","auditmanager:ListAssessmentReports","auditmanager:ListAssessments","auditmanager:ListControls","auditmanager:ListKeywordsForDataSource","auditmanager:ListNotifications","auditmanager:ListTagsForResource","auditmanager:ValidateAssessmentReportIntegrity","autoscaling-plans:Describe*","autoscaling-plans:GetScalingPlanResourceForecastData","autoscaling:Describe*","autoscaling:GetPredictiveScalingForecast","aws-portal:View*","backup-gateway:GetBandwidthRateLimitSchedule","backup-gateway:GetGateway","backup-gateway:GetHypervisor","backup-gateway:GetHypervisorPropertyMappings","backup-gateway:GetVirtualMachine","backup-gateway:ListGateways","backup-gateway:ListHypervisors","backup-gateway:ListTagsForResource","backup-gateway:ListVirtualMachines","backup:Describe*","backup:Get*","backup:List*","batch:Describe*","batch:List*","bedrock-agentcore:GetAgentRuntime","bedrock-agentcore:GetAgentRuntimeEndpoint","bedrock-agentcore:GetApiKeyCredentialProvider","bedrock-agentcore:GetBrowser","bedrock-agentcore:GetBrowserProfile","bedrock-agentcore:GetBrowserSession","bedrock-agentcore:GetCodeInterpreter","bedrock-agentcore:GetCodeInterpreterSession","bedrock-agentcore:GetEvaluator","bedrock-agentcore:GetEvent","bedrock-agentcore:GetGateway","bedrock-agentcore:GetGatewayTarget","bedrock-agentcore:GetMemory","bedrock-agentcore:GetMemoryRecord","bedrock-agentcore:GetOauth2CredentialProvider","bedrock-agentcore:GetOnlineEvaluationConfig","bedrock-agentcore:GetPolicy","bedrock-agentcore:GetPolicyEngine","bedrock-agentcore:GetPolicyGeneration","bedrock-agentcore:GetTokenVault","bedrock-agentcore:GetWorkloadIdentity","bedrock-agentcore:ListAgentRuntimeEndpoints","bedrock-agentcore:ListAgentRuntimes","bedrock-agentcore:ListAgentRuntimeVersions","bedrock-agentcore:ListApiKeyCredentialProviders","bedrock-agentcore:ListBrowserProfiles","bedrock-agentcore:ListBrowsers","bedrock-agentcore:ListBrowserSessions","bedrock-agentcore:ListCodeInterpreters","bedrock-agentcore:ListCodeInterpreterSessions","bedrock-agentcore:ListEvaluators","bedrock-agentcore:ListEvents","bedrock-agentcore:ListGateways","bedrock-agentcore:ListGatewayTargets","bedrock-agentcore:ListMemories","bedrock-agentcore:ListMemoryRecords","bedrock-agentcore:ListOauth2CredentialProviders","bedrock-agentcore:ListOnlineEvaluationConfigs","bedrock-agentcore:ListPolicies","bedrock-agentcore:ListPolicyEngines","bedrock-agentcore:ListPolicyGenerationAssets","bedrock-agentcore:ListPolicyGenerations","bedrock-agentcore:ListTagsForResource","bedrock-agentcore:ListWorkloadIdentities","bedrock-agentcore:RetrieveMemoryRecords","bedrock:GetAgent","bedrock:GetAgentActionGroup","bedrock:GetAgentAlias","bedrock:GetAgentCollaborator","bedrock:GetAgentKnowledgeBase","bedrock:GetAgentVersion","bedrock:GetCustomModel","bedrock:GetDataSource","bedrock:GetEvaluationJob","bedrock:GetFlow","bedrock:GetFlowAlias","bedrock:GetFlowVersion","bedrock:GetFoundationModel","bedrock:GetFoundationModelAvailability","bedrock:GetGuardrail","bedrock:GetInferenceProfile","bedrock:GetIngestionJob","bedrock:GetKnowledgeBase","bedrock:GetModelCustomizationJob","bedrock:GetModelInvocationJob","bedrock:GetModelInvocationLoggingConfiguration","bedrock:GetPrompt","bedrock:GetProvisionedModelThroughput","bedrock:GetResourcePolicy","bedrock:GetUseCaseForModelAccess","bedrock:ListAgentActionGroups","bedrock:ListAgentAliases","bedrock:ListAgentCollaborators","bedrock:ListAgentKnowledgeBases","bedrock:ListAgents","bedrock:ListAgentVersions","bedrock:ListCustomModels","bedrock:ListDataSources","bedrock:ListEnforcedGuardrailsConfiguration","bedrock:ListEvaluationJobs","bedrock:ListFlowAliases","bedrock:ListFlows","bedrock:ListFlowVersions","bedrock:ListFoundationModelAgreementOffers","bedrock:ListFoundationModels","bedrock:ListGuardrails","bedrock:ListInferenceProfiles","bedrock:ListIngestionJobs","bedrock:ListKnowledgeBases","bedrock:ListModelCustomizationJobs","bedrock:ListModelInvocationJobs","bedrock:ListPrompts","bedrock:ListProvisionedModelThroughputs","billing:GetBillingData","billing:GetBillingDetails","billing:GetBillingNotifications","billing:GetBillingPreferences","billing:GetBillingView","billing:GetContractInformation","billing:GetCredits","billing:GetIAMAccessPreference","billing:GetResourcePolicy","billing:GetSellerOfRecord","billing:ListBillingViews","billing:ListSourceViewsForBillingView","billing:ListTagsForResource","billingconductor:GetBillingGroupCostReport","billingconductor:ListAccountAssociations","billingconductor:ListBillingGroupCostReports","billingconductor:ListBillingGroups","billingconductor:ListCustomLineItems","billingconductor:ListCustomLineItemVersions","billingconductor:ListPricingPlans","billingconductor:ListPricingPlansAssociatedWithPricingRule","billingconductor:ListPricingRules","billingconductor:ListPricingRulesAssociatedToPricingPlan","billingconductor:ListResourcesAssociatedToCustomLineItem","billingconductor:ListTagsForResource","braket:GetDevice","braket:GetJob","braket:GetQuantumTask","braket:SearchDevices","braket:SearchJobs","braket:SearchQuantumTasks","braket:SearchSpendingLimits","budgets:Describe*","budgets:ListTagsForResource","budgets:View*","cassandra:Select","ce:DescribeCostCategoryDefinition","ce:DescribeNotificationSubscription","ce:DescribeReport","ce:GetAnomalies","ce:GetAnomalyMonitors","ce:GetAnomalySubscriptions","ce:GetApproximateUsageRecords","ce:GetCommitmentPurchaseAnalysis","ce:GetCostAndUsage","ce:GetCostAndUsageComparisons","ce:GetCostAndUsageWithResources","ce:GetCostCategories","ce:GetCostComparisonDrivers","ce:GetCostForecast","ce:GetDimensionValues","ce:GetPreferences","ce:GetReservationCoverage","ce:GetReservationPurchaseRecommendation","ce:GetReservationUtilization","ce:GetRightsizingRecommendation","ce:GetSavingsPlanPurchaseRecommendationDetails","ce:GetSavingsPlansCoverage","ce:GetSavingsPlansPurchaseRecommendation","ce:GetSavingsPlansUtilization","ce:GetSavingsPlansUtilizationDetails","ce:GetTags","ce:GetUsageForecast","ce:ListCommitmentPurchaseAnalyses","ce:ListCostAllocationTagBackfillHistory","ce:ListCostAllocationTags","ce:ListCostCategoryDefinitions","ce:ListCostCategoryResourceAssociations","ce:ListSavingsPlansPurchaseRecommendationGeneration","ce:ListTagsForResource","chatbot:Describe*","chatbot:Get*","chatbot:List*","chime:Get*","chime:List*","chime:Retrieve*","chime:Search*","chime:Validate*","cleanrooms-ml:GetAudienceGenerationJob","cleanrooms-ml:GetAudienceModel","cleanrooms-ml:GetConfiguredAudienceModel","cleanrooms-ml:GetConfiguredAudienceModelPolicy","cleanrooms-ml:GetTrainingDataset","cleanrooms-ml:ListAudienceExportJobs","cleanrooms-ml:ListAudienceGenerationJobs","cleanrooms-ml:ListAudienceModels","cleanrooms-ml:ListConfiguredAudienceModels","cleanrooms-ml:ListTagsForResource","cleanrooms-ml:ListTrainingDatasets","cleanrooms:BatchGetCollaborationAnalysisTemplate","cleanrooms:BatchGetSchema","cleanrooms:BatchGetSchemaAnalysisRule","cleanrooms:GetAnalysisTemplate","cleanrooms:GetCollaboration","cleanrooms:GetCollaborationAnalysisTemplate","cleanrooms:GetCollaborationChangeRequest","cleanrooms:GetCollaborationConfiguredAudienceModelAssociation","cleanrooms:GetCollaborationIdNamespaceAssociation","cleanrooms:GetCollaborationPrivacyBudgetTemplate","cleanrooms:GetConfiguredAudienceModelAssociation","cleanrooms:GetConfiguredTable","cleanrooms:GetConfiguredTableAnalysisRule","cleanrooms:GetConfiguredTableAssociation","cleanrooms:GetConfiguredTableAssociationAnalysisRule","cleanrooms:GetIdMappingTable","cleanrooms:GetIdNamespaceAssociation","cleanrooms:GetMembership","cleanrooms:GetPrivacyBudgetTemplate","cleanrooms:GetProtectedJob","cleanrooms:GetProtectedQuery","cleanrooms:GetSchema","cleanrooms:GetSchemaAnalysisRule","cleanrooms:ListAnalysisTemplates","cleanrooms:ListCollaborationAnalysisTemplates","cleanrooms:ListCollaborationChangeRequests","cleanrooms:ListCollaborationConfiguredAudienceModelAssociations","cleanrooms:ListCollaborationIdNamespaceAssociations","cleanrooms:ListCollaborationPrivacyBudgets","cleanrooms:ListCollaborationPrivacyBudgetTemplates","cleanrooms:ListCollaborations","cleanrooms:ListConfiguredAudienceModelAssociations","cleanrooms:ListConfiguredTableAssociations","cleanrooms:ListConfiguredTables","cleanrooms:ListIdMappingTables","cleanrooms:ListIdNamespaceAssociations","cleanrooms:ListMembers","cleanrooms:ListMemberships","cleanrooms:ListPrivacyBudgets","cleanrooms:ListPrivacyBudgetTemplates","cleanrooms:ListProtectedJobs","cleanrooms:ListProtectedQueries","cleanrooms:ListSchemas","cleanrooms:ListTagsForResource","cleanrooms:PreviewPrivacyImpact","cloud9:Describe*","cloud9:List*","clouddirectory:BatchRead","clouddirectory:Get*","clouddirectory:List*","clouddirectory:LookupPolicy","cloudformation:BatchDescribeTypeConfigurations","cloudformation:Describe*","cloudformation:Detect*","cloudformation:Estimate*","cloudformation:Get*","cloudformation:List*","cloudformation:ValidateTemplate","cloudfront-keyvaluestore:Describe*","cloudfront-keyvaluestore:Get*","cloudfront-keyvaluestore:List*","cloudfront:Describe*","cloudfront:Get*","cloudfront:List*","cloudhsm:Describe*","cloudhsm:GetResourcePolicy","cloudhsm:List*","cloudsearch:Describe*","cloudsearch:List*","cloudtrail:Describe*","cloudtrail:Get*","cloudtrail:List*","cloudtrail:LookupEvents","cloudwatch:Describe*","cloudwatch:GenerateQuery","cloudwatch:GenerateQueryResultsSummary","cloudwatch:Get*","cloudwatch:List*","codeartifact:DescribeDomain","codeartifact:DescribePackage","codeartifact:DescribePackageVersion","codeartifact:DescribeRepository","codeartifact:GetAuthorizationToken","codeartifact:GetDomainPermissionsPolicy","codeartifact:GetPackageVersionAsset","codeartifact:GetPackageVersionReadme","codeartifact:GetRepositoryEndpoint","codeartifact:GetRepositoryPermissionsPolicy","codeartifact:ListDomains","codeartifact:ListPackages","codeartifact:ListPackageVersionAssets","codeartifact:ListPackageVersionDependencies","codeartifact:ListPackageVersions","codeartifact:ListRepositories","codeartifact:ListRepositoriesInDomain","codeartifact:ListTagsForResource","codeartifact:ReadFromRepository","codebuild:BatchGet*","codebuild:DescribeCodeCoverages","codebuild:DescribeTestCases","codebuild:List*","codecatalyst:GetBillingAuthorization","codecatalyst:GetConnection","codecatalyst:GetPendingConnection","codecatalyst:ListConnections","codecatalyst:ListIamRolesForConnection","codecatalyst:ListTagsForResource","codecommit:BatchGet*","codecommit:Describe*","codecommit:Get*","codecommit:GitPull","codecommit:List*","codeconnections:GetConnection","codeconnections:GetHost","codeconnections:GetRepositoryLink","codeconnections:GetRepositorySyncStatus","codeconnections:GetResourceSyncStatus","codeconnections:GetSyncConfiguration","codeconnections:ListConnections","codeconnections:ListHosts","codeconnections:ListRepositoryLinks","codeconnections:ListRepositorySyncDefinitions","codeconnections:ListSyncConfigurations","codeconnections:ListTagsForResource","codedeploy:BatchGet*","codedeploy:Get*","codedeploy:List*","codeguru-profiler:Describe*","codeguru-profiler:Get*","codeguru-profiler:List*","codeguru-reviewer:Describe*","codeguru-reviewer:Get*","codeguru-reviewer:List*","codepipeline:Get*","codepipeline:List*","codestar-connections:GetConnection","codestar-connections:GetHost","codestar-connections:GetRepositoryLink","codestar-connections:GetRepositorySyncStatus","codestar-connections:GetResourceSyncStatus","codestar-connections:GetSyncConfiguration","codestar-connections:ListConnections","codestar-connections:ListHosts","codestar-connections:ListRepositoryLinks","codestar-connections:ListRepositorySyncDefinitions","codestar-connections:ListSyncConfigurations","codestar-connections:ListTagsForResource","codestar-notifications:describeNotificationRule","codestar-notifications:listEventTypes","codestar-notifications:listNotificationRules","codestar-notifications:listTagsForResource","codestar-notifications:ListTargets","codestar:Describe*","codestar:Get*","codestar:List*","codestar:Verify*","codewhisperer:ListProfiles","cognito-identity:Describe*","cognito-identity:GetCredentialsForIdentity","cognito-identity:GetIdentityPoolAnalytics","cognito-identity:GetIdentityPoolDailyAnalytics","cognito-identity:GetIdentityPoolRoles","cognito-identity:GetIdentityProviderDailyAnalytics","cognito-identity:GetOpenIdToken","cognito-identity:GetOpenIdTokenForDeveloperIdentity","cognito-identity:List*","cognito-identity:Lookup*","cognito-idp:AdminGet*","cognito-idp:AdminList*","cognito-idp:Describe*","cognito-idp:Get*","cognito-idp:List*","cognito-sync:Describe*","cognito-sync:Get*","cognito-sync:List*","cognito-sync:QueryRecords","comprehend:BatchDetect*","comprehend:Classify*","comprehend:Contains*","comprehend:Describe*","comprehend:Detect*","comprehend:List*","compute-optimizer:DescribeRecommendationExportJobs","compute-optimizer:GetAutoScalingGroupRecommendations","compute-optimizer:GetEBSVolumeRecommendations","compute-optimizer:GetEC2InstanceRecommendations","compute-optimizer:GetEC2RecommendationProjectedMetrics","compute-optimizer:GetECSServiceRecommendationProjectedMetrics","compute-optimizer:GetECSServiceRecommendations","compute-optimizer:GetEffectiveRecommendationPreferences","compute-optimizer:GetEnrollmentStatus","compute-optimizer:GetEnrollmentStatusesForOrganization","compute-optimizer:GetIdleRecommendations","compute-optimizer:GetLambdaFunctionRecommendations","compute-optimizer:GetLicenseRecommendations","compute-optimizer:GetRDSDatabaseRecommendationProjectedMetrics","compute-optimizer:GetRDSDatabaseRecommendations","compute-optimizer:GetRecommendationPreferences","compute-optimizer:GetRecommendationSummaries","config:BatchGetAggregateResourceConfig","config:BatchGetResourceConfig","config:Deliver*","config:Describe*","config:Get*","config:List*","config:SelectAggregateResourceConfig","config:SelectResourceConfig","connect:Describe*","connect:GetContactAttributes","connect:GetCurrentMetricData","connect:GetCurrentUserData","connect:GetFederationToken","connect:GetMetricData","connect:GetMetricDataV2","connect:GetTaskTemplate","connect:GetTrafficDistribution","connect:List*","consoleapp:GetDeviceIdentity","consoleapp:ListDeviceIdentities","consolidatedbilling:GetAccountBillingRole","consolidatedbilling:ListLinkedAccounts","controlcatalog:GetControl","controlcatalog:ListCommonControls","controlcatalog:ListControlMappings","controlcatalog:ListControls","controlcatalog:ListDomains","controlcatalog:ListObjectives","cost-optimization-hub:GetPreferences","cost-optimization-hub:GetRecommendation","cost-optimization-hub:ListEfficiencyMetrics","cost-optimization-hub:ListEnrollmentStatuses","cost-optimization-hub:ListRecommendations","cost-optimization-hub:ListRecommendationSummaries","cur:GetClassicReport","cur:GetClassicReportPreferences","cur:GetUsageReport","customer-verification:GetCustomerVerificationDetails","customer-verification:GetCustomerVerificationEligibility","databrew:DescribeDataset","databrew:DescribeJob","databrew:DescribeJobRun","databrew:DescribeProject","databrew:DescribeRecipe","databrew:DescribeRuleset","databrew:DescribeSchedule","databrew:ListDatasets","databrew:ListJobRuns","databrew:ListJobs","databrew:ListProjects","databrew:ListRecipes","databrew:ListRecipeVersions","databrew:ListRulesets","databrew:ListSchedules","databrew:ListTagsForResource","dataexchange:Get*","dataexchange:List*","datapipeline:Describe*","datapipeline:EvaluateExpression","datapipeline:Get*","datapipeline:List*","datapipeline:QueryObjects","datapipeline:Validate*","datasync:Describe*","datasync:List*","datazone:GetAsset","datazone:GetAssetType","datazone:GetDataProduct","datazone:GetDataSource","datazone:GetDataSourceRun","datazone:GetDomain","datazone:GetDomainSharingPolicy","datazone:GetDomainUnit","datazone:GetEnvironment","datazone:GetEnvironmentAction","datazone:GetEnvironmentBlueprint","datazone:GetEnvironmentBlueprintConfiguration","datazone:GetEnvironmentProfile","datazone:GetFormType","datazone:GetGlossary","datazone:GetGlossaryTerm","datazone:GetGroupProfile","datazone:GetLineageNode","datazone:GetListing","datazone:GetMetadataGenerationRun","datazone:GetProject","datazone:GetProjectProfile","datazone:GetSubscription","datazone:GetSubscriptionEligibility","datazone:GetSubscriptionGrant","datazone:GetSubscriptionRequestDetails","datazone:GetSubscriptionTarget","datazone:GetTimeSeriesDataPoint","datazone:GetUserProfile","datazone:ListAccountEnvironments","datazone:ListAssetRevisions","datazone:ListDataProductRevisions","datazone:ListDataSourceRunActivities","datazone:ListDataSourceRuns","datazone:ListDataSources","datazone:ListDomains","datazone:ListDomainUnitsForParent","datazone:ListEntityOwners","datazone:ListEnvironmentActions","datazone:ListEnvironmentBlueprintConfigurations","datazone:ListEnvironmentBlueprintConfigurationSummaries","datazone:ListEnvironmentBlueprints","datazone:ListEnvironmentProfiles","datazone:ListEnvironments","datazone:ListGroupsForUser","datazone:ListLineageNodeHistory","datazone:ListNotifications","datazone:ListPolicyGrants","datazone:ListProjectMemberships","datazone:ListProjectProfiles","datazone:ListProjects","datazone:ListSubscriptionGrants","datazone:ListSubscriptionRequests","datazone:ListSubscriptions","datazone:ListSubscriptionTargets","datazone:ListTagsForResource","datazone:ListTimeSeriesDataPoints","datazone:Search","datazone:SearchGroupProfiles","datazone:SearchListings","datazone:SearchTypes","datazone:SearchUserProfiles","dax:BatchGetItem","dax:Describe*","dax:GetItem","dax:ListTags","dax:Query","dax:Scan","deadline:BatchGetJobEntity","deadline:GetApplicationVersion","deadline:GetBudget","deadline:GetFarm","deadline:GetFleet","deadline:GetJob","deadline:GetLicenseEndpoint","deadline:GetMonitor","deadline:GetQueue","deadline:GetQueueEnvironment","deadline:GetQueueFleetAssociation","deadline:GetSession","deadline:GetSessionAction","deadline:GetSessionsStatisticsAggregation","deadline:GetStep","deadline:GetStorageProfile","deadline:GetStorageProfileForQueue","deadline:GetTask","deadline:GetWorker","deadline:ListAvailableMeteredProducts","deadline:ListBudgets","deadline:ListFarmMembers","deadline:ListFarms","deadline:ListFleetMembers","deadline:ListFleets","deadline:ListJobMembers","deadline:ListJobParameterDefinitions","deadline:ListJobs","deadline:ListLicenseEndpoints","deadline:ListMeteredProducts","deadline:ListMonitors","deadline:ListQueueEnvironments","deadline:ListQueueFleetAssociations","deadline:ListQueueMembers","deadline:ListQueues","deadline:ListSessionActions","deadline:ListSessions","deadline:ListSessionsForWorker","deadline:ListStepConsumers","deadline:ListStepDependencies","deadline:ListSteps","deadline:ListStorageProfiles","deadline:ListStorageProfilesForQueue","deadline:ListTagsForResource","deadline:ListTasks","deadline:ListWorkers","deadline:SearchJobs","deadline:SearchSteps","deadline:SearchTasks","deadline:SearchWorkers","deepcomposer:GetComposition","deepcomposer:GetModel","deepcomposer:GetSampleModel","deepcomposer:ListCompositions","deepcomposer:ListModels","deepcomposer:ListSampleModels","deepcomposer:ListTrainingTopics","detective:BatchGetGraphMemberDatasources","detective:BatchGetMembershipDatasources","detective:Get*","detective:List*","detective:SearchGraph","devicefarm:Get*","devicefarm:List*","devops-guru:DescribeAccountHealth","devops-guru:DescribeAccountOverview","devops-guru:DescribeAnomaly","devops-guru:DescribeEventSourcesConfig","devops-guru:DescribeFeedback","devops-guru:DescribeInsight","devops-guru:DescribeOrganizationHealth","devops-guru:DescribeOrganizationOverview","devops-guru:DescribeOrganizationResourceCollectionHealth","devops-guru:DescribeResourceCollectionHealth","devops-guru:DescribeServiceIntegration","devops-guru:GetCostEstimation","devops-guru:GetResourceCollection","devops-guru:ListAnomaliesForInsight","devops-guru:ListAnomalousLogGroups","devops-guru:ListEvents","devops-guru:ListInsights","devops-guru:ListMonitoredResources","devops-guru:ListNotificationChannels","devops-guru:ListOrganizationInsights","devops-guru:ListRecommendations","devops-guru:SearchInsights","devops-guru:StartCostEstimation","directconnect:Describe*","discovery:Describe*","discovery:Get*","discovery:List*","dlm:Get*","dms:Describe*","dms:List*","dms:Test*","docdb-elastic:GetCluster","docdb-elastic:GetClusterSnapshot","docdb-elastic:ListClusters","docdb-elastic:ListClusterSnapshots","docdb-elastic:ListPendingMaintenanceActions","docdb-elastic:ListTagsForResource","drs:DescribeJobLogItems","drs:DescribeJobs","drs:DescribeLaunchConfigurationTemplates","drs:DescribeRecoveryInstances","drs:DescribeRecoverySnapshots","drs:DescribeReplicationConfigurationTemplates","drs:DescribeSourceNetworks","drs:DescribeSourceServers","drs:GetFailbackReplicationConfiguration","drs:GetLaunchConfiguration","drs:GetReplicationConfiguration","drs:ListExtensibleSourceServers","drs:ListLaunchActions","drs:ListStagingAccounts","drs:ListTagsForResource","ds:Check*","ds:Describe*","ds:Get*","ds:List*","ds:Verify*","dsql:GetCluster","dsql:GetClusterPolicy","dsql:GetVpcEndpointServiceName","dsql:ListClusters","dsql:ListTagsForResource","dynamodb:BatchGet*","dynamodb:Describe*","dynamodb:Get*","dynamodb:List*","dynamodb:PartiQLSelect","dynamodb:Query","dynamodb:Scan","ec2:Describe*","ec2:DescribeInstanceImageMetadata","ec2:Get*","ec2:ListImagesInRecycleBin","ec2:ListSnapshotsInRecycleBin","ec2:SearchLocalGatewayRoutes","ec2:SearchTransitGatewayRoutes","ec2messages:Get*","ecr-public:BatchCheckLayerAvailability","ecr-public:DescribeImages","ecr-public:DescribeImageTags","ecr-public:DescribeRegistries","ecr-public:DescribeRepositories","ecr-public:GetAuthorizationToken","ecr-public:GetRegistryCatalogData","ecr-public:GetRepositoryCatalogData","ecr-public:GetRepositoryPolicy","ecr-public:ListTagsForResource","ecr:BatchCheck*","ecr:BatchGet*","ecr:Describe*","ecr:Get*","ecr:List*","ecs:Describe*","ecs:GetTaskProtection","ecs:List*","eks:AccessKubernetesApi","eks:Describe*","eks:List*","elasticache:Describe*","elasticache:List*","elasticbeanstalk:Check*","elasticbeanstalk:Describe*","elasticbeanstalk:List*","elasticbeanstalk:Request*","elasticbeanstalk:Retrieve*","elasticbeanstalk:Validate*","elasticfilesystem:Describe*","elasticfilesystem:ListTagsForResource","elasticloadbalancing:Describe*","elasticmapreduce:Describe*","elasticmapreduce:GetBlockPublicAccessConfiguration","elasticmapreduce:List*","elasticmapreduce:View*","elastictranscoder:List*","elastictranscoder:Read*","elemental-appliances-software:Get*","elemental-appliances-software:List*","elemental-inference:GetFeed","elemental-inference:ListFeeds","emr-containers:DescribeJobRun","emr-containers:DescribeManagedEndpoint","emr-containers:DescribeVirtualCluster","emr-containers:ListJobRuns","emr-containers:ListManagedEndpoints","emr-containers:ListTagsForResource","emr-containers:ListVirtualClusters","emr-serverless:GetApplication","emr-serverless:GetDashboardForJobRun","emr-serverless:GetJobRun","emr-serverless:ListApplications","emr-serverless:ListJobRuns","emr-serverless:ListTagsForResource","es:Describe*","es:ESHttpGet","es:ESHttpHead","es:Get*","es:List*","events:Describe*","events:List*","events:Test*","evidently:GetExperiment","evidently:GetExperimentResults","evidently:GetFeature","evidently:GetLaunch","evidently:GetProject","evidently:GetSegment","evidently:ListExperiments","evidently:ListFeatures","evidently:ListLaunches","evidently:ListProjects","evidently:ListSegmentReferences","evidently:ListSegments","evidently:ListTagsForResource","evidently:TestSegmentPattern","firehose:Describe*","firehose:List*","fis:GetAction","fis:GetExperiment","fis:GetExperimentTargetAccountConfiguration","fis:GetExperimentTemplate","fis:GetTargetAccountConfiguration","fis:GetTargetResourceType","fis:ListActions","fis:ListExperimentResolvedTargets","fis:ListExperiments","fis:ListExperimentTargetAccountConfigurations","fis:ListExperimentTemplates","fis:ListTagsForResource","fis:ListTargetAccountConfigurations","fis:ListTargetResourceTypes","fms:GetAdminAccount","fms:GetAdminScope","fms:GetAppsList","fms:GetComplianceDetail","fms:GetNotificationChannel","fms:GetPolicy","fms:GetProtectionStatus","fms:GetProtocolsList","fms:GetViolationDetails","fms:ListAppsLists","fms:ListComplianceStatus","fms:ListMemberAccounts","fms:ListPolicies","fms:ListProtocolsLists","fms:ListTagsForResource","forecast:DescribeAutoPredictor","forecast:DescribeDataset","forecast:DescribeDatasetGroup","forecast:DescribeDatasetImportJob","forecast:DescribeExplainability","forecast:DescribeExplainabilityExport","forecast:DescribeForecast","forecast:DescribeForecastExportJob","forecast:DescribeMonitor","forecast:DescribePredictor","forecast:DescribePredictorBacktestExportJob","forecast:DescribeWhatIfAnalysis","forecast:DescribeWhatIfForecast","forecast:DescribeWhatIfForecastExport","forecast:GetAccuracyMetrics","forecast:ListDatasetGroups","forecast:ListDatasetImportJobs","forecast:ListDatasets","forecast:ListExplainabilities","forecast:ListExplainabilityExports","forecast:ListForecastExportJobs","forecast:ListForecasts","forecast:ListMonitorEvaluations","forecast:ListMonitors","forecast:ListPredictorBacktestExportJobs","forecast:ListPredictors","forecast:ListWhatIfAnalyses","forecast:ListWhatIfForecastExports","forecast:ListWhatIfForecasts","forecast:QueryForecast","forecast:QueryWhatIfForecast","frauddetector:BatchGetVariable","frauddetector:DescribeDetector","frauddetector:DescribeModelVersions","frauddetector:GetBatchImportJobs","frauddetector:GetBatchPredictionJobs","frauddetector:GetDeleteEventsByEventTypeStatus","frauddetector:GetDetectors","frauddetector:GetDetectorVersion","frauddetector:GetEntityTypes","frauddetector:GetEvent","frauddetector:GetEventPredictionMetadata","frauddetector:GetEventTypes","frauddetector:GetExternalModels","frauddetector:GetKMSEncryptionKey","frauddetector:GetLabels","frauddetector:GetListElements","frauddetector:GetListsMetadata","frauddetector:GetModels","frauddetector:GetModelVersion","frauddetector:GetOutcomes","frauddetector:GetRules","frauddetector:GetVariables","frauddetector:ListEventPredictions","frauddetector:ListTagsForResource","freertos:Describe*","freertos:List*","freetier:GetAccountActivity","freetier:GetAccountPlanState","freetier:GetFreeTierAlertPreference","freetier:GetFreeTierUsage","freetier:ListAccountActivities","fsx:Describe*","fsx:List*","gamelift:Describe*","gamelift:Get*","gamelift:List*","gamelift:ResolveAlias","gamelift:Search*","gameliftstreams:GetApplication","gameliftstreams:GetStreamGroup","gameliftstreams:ListApplications","gameliftstreams:ListStreamGroups","gameliftstreams:ListStreamSessions","gameliftstreams:ListStreamSessionsByAccount","gameliftstreams:ListTagsForResource","glacier:Describe*","glacier:Get*","glacier:List*","globalaccelerator:Describe*","globalaccelerator:List*","glue:BatchGetCrawlers","glue:BatchGetDevEndpoints","glue:BatchGetJobs","glue:BatchGetPartition","glue:BatchGetTableOptimizer","glue:BatchGetTriggers","glue:BatchGetWorkflows","glue:CheckSchemaVersionValidity","glue:GetCatalog","glue:GetCatalogImportStatus","glue:GetCatalogs","glue:GetClassifier","glue:GetClassifiers","glue:GetCrawler","glue:GetCrawlerMetrics","glue:GetCrawlers","glue:GetDatabase","glue:GetDatabases","glue:GetDataCatalogEncryptionSettings","glue:GetDataflowGraph","glue:GetDevEndpoint","glue:GetDevEndpoints","glue:GetJob","glue:GetJobBookmark","glue:GetJobRun","glue:GetJobRuns","glue:GetJobs","glue:GetMapping","glue:GetMLTaskRun","glue:GetMLTaskRuns","glue:GetMLTransform","glue:GetMLTransforms","glue:GetPartition","glue:GetPartitions","glue:GetPlan","glue:GetRegistry","glue:GetResourcePolicy","glue:GetSchema","glue:GetSchemaByDefinition","glue:GetSchemaVersion","glue:GetSchemaVersionsDiff","glue:GetSecurityConfiguration","glue:GetSecurityConfigurations","glue:GetSession","glue:GetStatement","glue:GetTable","glue:GetTableOptimizer","glue:GetTables","glue:GetTableVersion","glue:GetTableVersions","glue:GetTags","glue:GetTrigger","glue:GetTriggers","glue:GetUserDefinedFunction","glue:GetUserDefinedFunctions","glue:GetWorkflow","glue:GetWorkflowRun","glue:GetWorkflowRunProperties","glue:GetWorkflowRuns","glue:ListCrawlers","glue:ListCrawls","glue:ListDevEndpoints","glue:ListJobs","glue:ListMLTransforms","glue:ListRegistries","glue:ListSchemas","glue:ListSchemaVersions","glue:ListSessions","glue:ListStatements","glue:ListTableOptimizerRuns","glue:ListTriggers","glue:ListWorkflows","glue:QuerySchemaVersionMetadata","glue:SearchTables","grafana:DescribeWorkspace","grafana:DescribeWorkspaceAuthentication","grafana:DescribeWorkspaceConfiguration","grafana:ListPermissions","grafana:ListTagsForResource","grafana:ListVersions","grafana:ListWorkspaces","greengrass:DescribeComponent","greengrass:Get*","greengrass:List*","groundstation:DescribeContactVersion","groundstation:DescribeEphemeris","groundstation:GetConfig","groundstation:GetDataflowEndpointGroup","groundstation:GetMinuteUsage","groundstation:GetMissionProfile","groundstation:GetSatellite","groundstation:ListAntennas","groundstation:ListConfigs","groundstation:ListContacts","groundstation:ListContactVersions","groundstation:ListDataflowEndpointGroups","groundstation:ListEphemerides","groundstation:ListGroundStationReservations","groundstation:ListGroundStations","groundstation:ListMissionProfiles","groundstation:ListSatellites","groundstation:ListTagsForResource","groundstation:DescribeContact","guardduty:Describe*","guardduty:Get*","guardduty:List*","health:Describe*","healthlake:DescribeFHIRDatastore","healthlake:DescribeFHIRExportJob","healthlake:DescribeFHIRImportJob","healthlake:GetCapabilities","healthlake:ListFHIRDatastores","healthlake:ListFHIRExportJobs","healthlake:ListFHIRImportJobs","healthlake:ListTagsForResource","healthlake:ReadResource","healthlake:SearchWithGet","healthlake:SearchWithPost","iam:Generate*","iam:Get*","iam:List*","iam:Simulate*","identity-sync:GetSyncProfile","identity-sync:GetSyncTarget","identity-sync:ListSyncFilters","identitystore-auth:BatchGetSession","identitystore-auth:ListSessions","identitystore:DescribeGroup","identitystore:DescribeGroupMembership","identitystore:DescribeUser","identitystore:GetGroupId","identitystore:GetGroupMembershipId","identitystore:GetUserId","identitystore:IsMemberInGroups","identitystore:ListGroupMemberships","identitystore:ListGroupMembershipsForMember","identitystore:ListGroups","identitystore:ListUsers","imagebuilder:Get*","imagebuilder:List*","importexport:Get*","importexport:List*","inspector:Describe*","inspector:Get*","inspector:List*","inspector:Preview*","inspector2:BatchGetAccountStatus","inspector2:BatchGetCodeSnippet","inspector2:BatchGetFreeTrialInfo","inspector2:BatchGetMemberEc2DeepInspectionStatus","inspector2:DescribeOrganizationConfiguration","inspector2:GetCisScanReport","inspector2:GetConfiguration","inspector2:GetDelegatedAdminAccount","inspector2:GetEc2DeepInspectionConfiguration","inspector2:GetEncryptionKey","inspector2:GetFindingsReportStatus","inspector2:GetMember","inspector2:GetSbomExport","inspector2:ListAccountPermissions","inspector2:ListCisScanConfigurations","inspector2:ListCisScans","inspector2:ListCoverage","inspector2:ListCoverageStatistics","inspector2:ListDelegatedAdminAccounts","inspector2:ListFilters","inspector2:ListFindingAggregations","inspector2:ListFindings","inspector2:ListMembers","inspector2:ListTagsForResource","inspector2:ListUsageTotals","inspector2:SearchVulnerabilities","internetmonitor:GetHealthEvent","internetmonitor:GetInternetEvent","internetmonitor:GetMonitor","internetmonitor:ListHealthEvents","internetmonitor:ListInternetEvents","internetmonitor:ListMonitors","internetmonitor:ListTagsForResource","interconnect:DescribeConnectionProposal","interconnect:GetConnection","interconnect:GetEnvironment","interconnect:ListAttachPoints","interconnect:ListTagsForResource","interconnect:ListEnvironments","interconnect:ListConnections","invoicing:GetInvoiceEmailDeliveryPreferences","invoicing:GetInvoicePDF","invoicing:ListInvoiceSummaries","iot:Describe*","iot:Get*","iot:List*","iot1click:DescribeDevice","iot1click:DescribePlacement","iot1click:DescribeProject","iot1click:GetDeviceMethods","iot1click:GetDevicesInPlacement","iot1click:ListDeviceEvents","iot1click:ListDevices","iot1click:ListPlacements","iot1click:ListProjects","iot1click:ListTagsForResource","iotanalytics:Describe*","iotanalytics:Get*","iotanalytics:List*","iotanalytics:SampleChannelData","iotevents:DescribeAlarm","iotevents:DescribeAlarmModel","iotevents:DescribeDetector","iotevents:DescribeDetectorModel","iotevents:DescribeInput","iotevents:DescribeLoggingOptions","iotevents:ListAlarmModels","iotevents:ListAlarmModelVersions","iotevents:ListAlarms","iotevents:ListDetectorModels","iotevents:ListDetectorModelVersions","iotevents:ListDetectors","iotevents:ListInputs","iotevents:ListTagsForResource","iotfleethub:DescribeApplication","iotfleethub:ListApplications","iotfleetwise:GetCampaign","iotfleetwise:GetDecoderManifest","iotfleetwise:GetFleet","iotfleetwise:GetLoggingOptions","iotfleetwise:GetModelManifest","iotfleetwise:GetRegisterAccountStatus","iotfleetwise:GetSignalCatalog","iotfleetwise:GetVehicle","iotfleetwise:GetVehicleStatus","iotfleetwise:ListCampaigns","iotfleetwise:ListDecoderManifestNetworkInterfaces","iotfleetwise:ListDecoderManifests","iotfleetwise:ListDecoderManifestSignals","iotfleetwise:ListFleets","iotfleetwise:ListFleetsForVehicle","iotfleetwise:ListModelManifestNodes","iotfleetwise:ListModelManifests","iotfleetwise:ListSignalCatalogNodes","iotfleetwise:ListSignalCatalogs","iotfleetwise:ListTagsForResource","iotfleetwise:ListVehicles","iotfleetwise:ListVehiclesInFleet","iotsitewise:Describe*","iotsitewise:Get*","iotsitewise:List*","iotwireless:GetDestination","iotwireless:GetDeviceProfile","iotwireless:GetEventConfigurationByResourceTypes","iotwireless:GetFuotaTask","iotwireless:GetLogLevelsByResourceTypes","iotwireless:GetMetricConfiguration","iotwireless:GetMetrics","iotwireless:GetMulticastGroup","iotwireless:GetMulticastGroupSession","iotwireless:GetNetworkAnalyzerConfiguration","iotwireless:GetPartnerAccount","iotwireless:GetPosition","iotwireless:GetPositionConfiguration","iotwireless:GetPositionEstimate","iotwireless:GetResourceEventConfiguration","iotwireless:GetResourceLogLevel","iotwireless:GetResourcePosition","iotwireless:GetServiceEndpoint","iotwireless:GetServiceProfile","iotwireless:GetWirelessDevice","iotwireless:GetWirelessDeviceImportTask","iotwireless:GetWirelessDeviceStatistics","iotwireless:GetWirelessGateway","iotwireless:GetWirelessGatewayCertificate","iotwireless:GetWirelessGatewayFirmwareInformation","iotwireless:GetWirelessGatewayStatistics","iotwireless:GetWirelessGatewayTask","iotwireless:GetWirelessGatewayTaskDefinition","iotwireless:ListDestinations","iotwireless:ListDeviceProfiles","iotwireless:ListDevicesForWirelessDeviceImportTask","iotwireless:ListEventConfigurations","iotwireless:ListFuotaTasks","iotwireless:ListMulticastGroups","iotwireless:ListMulticastGroupsByFuotaTask","iotwireless:ListNetworkAnalyzerConfigurations","iotwireless:ListPartnerAccounts","iotwireless:ListPositionConfigurations","iotwireless:ListQueuedMessages","iotwireless:ListServiceProfiles","iotwireless:ListTagsForResource","iotwireless:ListWirelessDeviceImportTasks","iotwireless:ListWirelessDevices","iotwireless:ListWirelessGateways","iotwireless:ListWirelessGatewayTaskDefinitions","ivs:BatchGetChannel","ivs:GetChannel","ivs:GetComposition","ivs:GetEncoderConfiguration","ivs:GetIngestConfiguration","ivs:GetParticipant","ivs:GetPlaybackKeyPair","ivs:GetPlaybackRestrictionPolicy","ivs:GetPublicKey","ivs:GetRecordingConfiguration","ivs:GetStage","ivs:GetStageSession","ivs:GetStorageConfiguration","ivs:GetStream","ivs:GetStreamSession","ivs:ListChannels","ivs:ListCompositions","ivs:ListEncoderConfigurations","ivs:ListIngestConfigurations","ivs:ListParticipantEvents","ivs:ListParticipants","ivs:ListPlaybackKeyPairs","ivs:ListPlaybackRestrictionPolicies","ivs:ListPublicKeys","ivs:ListRecordingConfigurations","ivs:ListStages","ivs:ListStageSessions","ivs:ListStorageConfigurations","ivs:ListStreamKeys","ivs:ListStreams","ivs:ListStreamSessions","ivs:ListTagsForResource","ivschat:GetLoggingConfiguration","ivschat:GetRoom","ivschat:ListLoggingConfigurations","ivschat:ListRooms","ivschat:ListTagsForResource"],"Resource":"*"},{"Sid":"ReadOnlyActionsGroup2","Effect":"Allow","Action":["kafka:Describe*","kafka:DescribeCluster","kafka:DescribeClusterOperation","kafka:DescribeClusterV2","kafka:DescribeConfiguration","kafka:DescribeConfigurationRevision","kafka:Get*","kafka:GetBootstrapBrokers","kafka:GetCompatibleKafkaVersions","kafka:List*","kafka:ListClusterOperations","kafka:ListClusters","kafka:ListClustersV2","kafka:ListConfigurationRevisions","kafka:ListConfigurations","kafka:ListKafkaVersions","kafka:ListNodes","kafka:ListTagsForResource","kafkaconnect:DescribeConnector","kafkaconnect:DescribeCustomPlugin","kafkaconnect:DescribeWorkerConfiguration","kafkaconnect:ListConnectors","kafkaconnect:ListCustomPlugins","kafkaconnect:ListWorkerConfigurations","kendra:BatchGetDocumentStatus","kendra:DescribeDataSource","kendra:DescribeExperience","kendra:DescribeFaq","kendra:DescribeIndex","kendra:DescribePrincipalMapping","kendra:DescribeQuerySuggestionsBlockList","kendra:DescribeQuerySuggestionsConfig","kendra:DescribeThesaurus","kendra:GetQuerySuggestions","kendra:GetSnapshots","kendra:ListDataSources","kendra:ListDataSourceSyncJobs","kendra:ListEntityPersonas","kendra:ListExperienceEntities","kendra:ListExperiences","kendra:ListFaqs","kendra:ListGroupsOlderThanOrderingId","kendra:ListIndices","kendra:ListQuerySuggestionsBlockLists","kendra:ListTagsForResource","kendra:ListThesauri","kendra:Query","kinesis:Describe*","kinesis:Get*","kinesis:List*","kinesisanalytics:Describe*","kinesisanalytics:Discover*","kinesisanalytics:Get*","kinesisanalytics:List*","kinesisvideo:Describe*","kinesisvideo:Get*","kinesisvideo:List*","kms:Describe*","kms:Get*","kms:List*","lakeformation:DescribeResource","lakeformation:GetDataCellsFilter","lakeformation:GetDataLakeSettings","lakeformation:GetEffectivePermissionsForPath","lakeformation:GetLfTag","lakeformation:GetResourceLfTags","lakeformation:ListDataCellsFilter","lakeformation:ListLfTags","lakeformation:ListPermissions","lakeformation:ListResources","lakeformation:ListTableStorageOptimizers","lakeformation:SearchDatabasesByLfTags","lakeformation:SearchTablesByLfTags","lambda:Get*","lambda:List*","launchwizard:DescribeAdditionalNode","launchwizard:DescribeProvisionedApp","launchwizard:DescribeProvisioningEvents","launchwizard:DescribeSettingsSet","launchwizard:GetDeployment","launchwizard:GetInfrastructureSuggestion","launchwizard:GetIpAddress","launchwizard:GetResourceCostEstimate","launchwizard:GetResourceRecommendation","launchwizard:GetSettingsSet","launchwizard:GetWorkload","launchwizard:GetWorkloadAsset","launchwizard:GetWorkloadAssets","launchwizard:GetWorkloadDeploymentPattern","launchwizard:ListAdditionalNodes","launchwizard:ListAllowedResources","launchwizard:ListDeploymentEvents","launchwizard:ListDeployments","launchwizard:ListProvisionedApps","launchwizard:ListResourceCostEstimates","launchwizard:ListSettingsSets","launchwizard:ListTagsForResource","launchwizard:ListWorkloadDeploymentOptions","launchwizard:ListWorkloadDeploymentPatterns","launchwizard:ListWorkloads","lex:DescribeBot","lex:DescribeBotAlias","lex:DescribeBotChannel","lex:DescribeBotLocale","lex:DescribeBotReplica","lex:DescribeBotVersion","lex:DescribeExport","lex:DescribeImport","lex:DescribeIntent","lex:DescribeResourcePolicy","lex:DescribeSlot","lex:DescribeSlotType","lex:Get*","lex:ListBotAliases","lex:ListBotAliasReplicas","lex:ListBotChannels","lex:ListBotLocales","lex:ListBotReplicas","lex:ListBots","lex:ListBotVersionReplicas","lex:ListBotVersions","lex:ListBuiltInIntents","lex:ListBuiltInSlotTypes","lex:ListExports","lex:ListImports","lex:ListIntents","lex:ListSlots","lex:ListSlotTypes","lex:ListTagsForResource","license-manager:Get*","license-manager:List*","lightsail:GetActiveNames","lightsail:GetAlarms","lightsail:GetAutoSnapshots","lightsail:GetBlueprints","lightsail:GetBucketAccessKeys","lightsail:GetBucketBundles","lightsail:GetBucketMetricData","lightsail:GetBuckets","lightsail:GetBundles","lightsail:GetCertificates","lightsail:GetCloudFormationStackRecords","lightsail:GetContainerAPIMetadata","lightsail:GetContainerImages","lightsail:GetContainerServiceDeployments","lightsail:GetContainerServiceMetricData","lightsail:GetContainerServicePowers","lightsail:GetContainerServices","lightsail:GetDisk","lightsail:GetDisks","lightsail:GetDiskSnapshot","lightsail:GetDiskSnapshots","lightsail:GetDistributionBundles","lightsail:GetDistributionLatestCacheReset","lightsail:GetDistributionMetricData","lightsail:GetDistributions","lightsail:GetDomain","lightsail:GetDomains","lightsail:GetExportSnapshotRecords","lightsail:GetInstance","lightsail:GetInstanceMetricData","lightsail:GetInstancePortStates","lightsail:GetInstances","lightsail:GetInstanceSnapshot","lightsail:GetInstanceSnapshots","lightsail:GetInstanceState","lightsail:GetKeyPair","lightsail:GetKeyPairs","lightsail:GetLoadBalancer","lightsail:GetLoadBalancerMetricData","lightsail:GetLoadBalancers","lightsail:GetLoadBalancerTlsCertificates","lightsail:GetOperation","lightsail:GetOperations","lightsail:GetOperationsForResource","lightsail:GetRegions","lightsail:GetRelationalDatabase","lightsail:GetRelationalDatabaseBlueprints","lightsail:GetRelationalDatabaseBundles","lightsail:GetRelationalDatabaseEvents","lightsail:GetRelationalDatabaseLogEvents","lightsail:GetRelationalDatabaseLogStreams","lightsail:GetRelationalDatabaseMetricData","lightsail:GetRelationalDatabaseParameters","lightsail:GetRelationalDatabases","lightsail:GetRelationalDatabaseSnapshot","lightsail:GetRelationalDatabaseSnapshots","lightsail:GetStaticIp","lightsail:GetStaticIps","lightsail:Is*","logs:Describe*","logs:FilterLogEvents","logs:Get*","logs:ListAggregateLogGroupSummaries","logs:ListAnomalies","logs:ListEntitiesForLogGroup","logs:ListIntegrations","logs:ListLogAnomalyDetectors","logs:ListLogDeliveries","logs:ListLogGroupsForEntity","logs:ListLogGroupsForQuery","logs:ListScheduledQueries","logs:ListSourcesForS3TableIntegration","logs:ListTagsForResource","logs:ListTagsLogGroup","logs:StartLiveTail","logs:StartQuery","logs:StopLiveTail","logs:StopQuery","logs:TestMetricFilter","lookoutequipment:DescribeDataIngestionJob","lookoutequipment:DescribeDataset","lookoutequipment:DescribeInferenceScheduler","lookoutequipment:DescribeLabel","lookoutequipment:DescribeLabelGroup","lookoutequipment:DescribeModel","lookoutequipment:DescribeModelVersion","lookoutequipment:DescribeResourcePolicy","lookoutequipment:DescribeRetrainingScheduler","lookoutequipment:ListDataIngestionJobs","lookoutequipment:ListDatasets","lookoutequipment:ListInferenceEvents","lookoutequipment:ListInferenceExecutions","lookoutequipment:ListInferenceSchedulers","lookoutequipment:ListLabelGroups","lookoutequipment:ListLabels","lookoutequipment:ListModels","lookoutequipment:ListModelVersions","lookoutequipment:ListRetrainingSchedulers","lookoutequipment:ListSensorStatistics","lookoutequipment:ListTagsForResource","lookoutmetrics:Describe*","lookoutmetrics:Get*","lookoutmetrics:List*","lookoutvision:DescribeDataset","lookoutvision:DescribeModel","lookoutvision:DescribeModelPackagingJob","lookoutvision:DescribeProject","lookoutvision:ListDatasetEntries","lookoutvision:ListModelPackagingJobs","lookoutvision:ListModels","lookoutvision:ListProjects","lookoutvision:ListTagsForResource","m2:GetApplication","m2:GetApplicationVersion","m2:GetBatchJobExecution","m2:GetDataSetDetails","m2:GetDataSetImportTask","m2:GetDeployment","m2:GetEnvironment","m2:ListApplications","m2:ListApplicationVersions","m2:ListBatchJobDefinitions","m2:ListBatchJobExecutions","m2:ListDataSetImportHistory","m2:ListDataSets","m2:ListDeployments","m2:ListEngineVersions","m2:ListEnvironments","m2:ListTagsForResource","machinelearning:Describe*","machinelearning:Get*","macie2:BatchGetCustomDataIdentifiers","macie2:DescribeBuckets","macie2:DescribeClassificationJob","macie2:DescribeOrganizationConfiguration","macie2:GetAdministratorAccount","macie2:GetAllowList","macie2:GetAutomatedDiscoveryConfiguration","macie2:GetBucketStatistics","macie2:GetClassificationExportConfiguration","macie2:GetClassificationScope","macie2:GetCustomDataIdentifier","macie2:GetFindings","macie2:GetFindingsFilter","macie2:GetFindingsPublicationConfiguration","macie2:GetFindingStatistics","macie2:GetInvitationsCount","macie2:GetMacieSession","macie2:GetMember","macie2:GetResourceProfile","macie2:GetRevealConfiguration","macie2:GetSensitiveDataOccurrencesAvailability","macie2:GetSensitivityInspectionTemplate","macie2:GetUsageStatistics","macie2:GetUsageTotals","macie2:ListAllowLists","macie2:ListAutomatedDiscoveryAccounts","macie2:ListClassificationJobs","macie2:ListClassificationScopes","macie2:ListCustomDataIdentifiers","macie2:ListFindings","macie2:ListFindingsFilters","macie2:ListInvitations","macie2:ListMembers","macie2:ListOrganizationAdminAccounts","macie2:ListResourceProfileArtifacts","macie2:ListResourceProfileDetections","macie2:ListSensitivityInspectionTemplates","macie2:ListTagsForResource","macie2:SearchResources","managedblockchain:GetMember","managedblockchain:GetNetwork","managedblockchain:GetNode","managedblockchain:GetProposal","managedblockchain:ListInvitations","managedblockchain:ListMembers","managedblockchain:ListNetworks","managedblockchain:ListNodes","managedblockchain:ListProposals","managedblockchain:ListProposalVotes","managedblockchain:ListTagsForResource","mediaconnect:DescribeFlow","mediaconnect:DescribeFlowSourceMetadata","mediaconnect:DescribeFlowSourceThumbnail","mediaconnect:DescribeGateway","mediaconnect:DescribeGatewayInstance","mediaconnect:DescribeOffering","mediaconnect:DescribeReservation","mediaconnect:DiscoverGatewayPollEndpoint","mediaconnect:GetRouterInput","mediaconnect:GetRouterNetworkInterface","mediaconnect:GetRouterOutput","mediaconnect:ListBridges","mediaconnect:ListEntitlements","mediaconnect:ListFlows","mediaconnect:ListGatewayInstances","mediaconnect:ListGateways","mediaconnect:ListOfferings","mediaconnect:ListReservations","mediaconnect:ListRouterInputs","mediaconnect:ListRouterNetworkInterfaces","mediaconnect:ListRouterOutputs","mediaconnect:ListTagsForResource","mediaconvert:DescribeEndpoints","mediaconvert:Get*","mediaconvert:List*","mediaconvert:Probe","mediaconvert:SearchJobs","medialive:DescribeAccountConfiguration","medialive:DescribeChannel","medialive:DescribeChannelPlacementGroup","medialive:DescribeCluster","medialive:DescribeInput","medialive:DescribeInputDevice","medialive:DescribeInputDeviceThumbnail","medialive:DescribeInputSecurityGroup","medialive:DescribeMultiplex","medialive:DescribeMultiplexProgram","medialive:DescribeNetwork","medialive:DescribeOffering","medialive:DescribeReservation","medialive:DescribeSchedule","medialive:GetCloudWatchAlarmTemplate","medialive:GetCloudWatchAlarmTemplateGroup","medialive:GetEventBridgeRuleTemplate","medialive:GetEventBridgeRuleTemplateGroup","medialive:GetSignalMap","medialive:ListChannels","medialive:ListCloudWatchAlarmTemplateGroups","medialive:ListCloudWatchAlarmTemplates","medialive:ListEventBridgeRuleTemplateGroups","medialive:ListEventBridgeRuleTemplates","medialive:ListInputDevices","medialive:ListInputDeviceTransfers","medialive:ListInputs","medialive:ListInputSecurityGroups","medialive:ListMultiplexes","medialive:ListMultiplexPrograms","medialive:ListOfferings","medialive:ListReservations","medialive:ListSignalMaps","medialive:ListTagsForResource","mediapackage-vod:Describe*","mediapackage-vod:List*","mediapackage:Describe*","mediapackage:List*","mediapackagev2:GetChannel","mediapackagev2:GetChannelGroup","mediapackagev2:GetChannelPolicy","mediapackagev2:GetHeadObject","mediapackagev2:GetObject","mediapackagev2:GetOriginEndpoint","mediapackagev2:GetOriginEndpointPolicy","mediapackagev2:ListChannelGroups","mediapackagev2:ListChannels","mediapackagev2:ListOriginEndpoints","mediapackagev2:ListTagsForResource","mediastore:DescribeContainer","mediastore:DescribeObject","mediastore:GetContainerPolicy","mediastore:GetCorsPolicy","mediastore:GetLifecyclePolicy","mediastore:GetMetricPolicy","mediastore:GetObject","mediastore:ListContainers","mediastore:ListItems","mediastore:ListTagsForResource","memorydb:DescribeAcls","memorydb:DescribeClusters","memorydb:DescribeEngineVersions","memorydb:DescribeEvents","memorydb:DescribeMultiRegionClusters","memorydb:DescribeMultiRegionParameterGroups","memorydb:DescribeMultiRegionParameters","memorydb:DescribeParameterGroups","memorydb:DescribeParameters","memorydb:DescribeReservedNodes","memorydb:DescribeReservedNodesOfferings","memorydb:DescribeServiceUpdates","memorydb:DescribeSnapshots","memorydb:DescribeSubnetGroups","memorydb:DescribeUsers","memorydb:ListAllowedMultiRegionClusterUpdates","memorydb:ListAllowedNodeTypeUpdates","memorydb:ListTags","mgh:Describe*","mgh:GetHomeRegion","mgh:List*","mgn:DescribeJobLogItems","mgn:DescribeJobs","mgn:DescribeLaunchConfigurationTemplates","mgn:DescribeReplicationConfigurationTemplates","mgn:DescribeSourceServers","mgn:DescribeVcenterClients","mgn:GetLaunchConfiguration","mgn:GetReplicationConfiguration","mgn:ListApplications","mgn:ListSourceServerActions","mgn:ListTemplateActions","mgn:ListWaves","mobileanalytics:Get*","mobiletargeting:Get*","mobiletargeting:List*","monitron:GetProject","monitron:GetProjectAdminUser","monitron:ListProjects","monitron:ListTagsForResource","mpa:GetApprovalTeam","mpa:GetIdentitySource","mpa:GetPolicyVersion","mpa:GetResourcePolicy","mpa:GetSession","mpa:ListApprovalTeams","mpa:ListIdentitySources","mpa:ListPolicies","mpa:ListPolicyVersions","mpa:ListResourcePolicies","mpa:ListSessions","mpa:ListTagsForResource","mq:Describe*","mq:List*","network-firewall:DescribeFirewall","network-firewall:DescribeFirewallPolicy","network-firewall:DescribeLoggingConfiguration","network-firewall:DescribeProxy","network-firewall:DescribeProxyConfiguration","network-firewall:DescribeProxyRule","network-firewall:DescribeProxyRuleGroup","network-firewall:DescribeResourcePolicy","network-firewall:DescribeRuleGroup","network-firewall:DescribeRuleGroupMetadata","network-firewall:DescribeTLSInspectionConfiguration","network-firewall:ListFirewallPolicies","network-firewall:ListFirewalls","network-firewall:ListProxies","network-firewall:ListProxyConfigurations","network-firewall:ListProxyRuleGroups","network-firewall:ListRuleGroups","network-firewall:ListTagsForResource","network-firewall:ListTLSInspectionConfigurations","networkflowmonitor:GetMonitor","networkflowmonitor:GetScope","networkflowmonitor:ListMonitors","networkflowmonitor:ListScopes","networkmanager:DescribeGlobalNetworks","networkmanager:GetConnectAttachment","networkmanager:GetConnections","networkmanager:GetConnectPeer","networkmanager:GetConnectPeerAssociations","networkmanager:GetCoreNetwork","networkmanager:GetCoreNetworkChangeEvents","networkmanager:GetCoreNetworkChangeSet","networkmanager:GetCoreNetworkPolicy","networkmanager:GetCustomerGatewayAssociations","networkmanager:GetDevices","networkmanager:GetLinkAssociations","networkmanager:GetLinks","networkmanager:GetNetworkResourceCounts","networkmanager:GetNetworkResourceRelationships","networkmanager:GetNetworkResources","networkmanager:GetNetworkRoutes","networkmanager:GetNetworkTelemetry","networkmanager:GetResourcePolicy","networkmanager:GetRouteAnalysis","networkmanager:GetSites","networkmanager:GetSiteToSiteVpnAttachment","networkmanager:GetTransitGatewayConnectPeerAssociations","networkmanager:GetTransitGatewayPeering","networkmanager:GetTransitGatewayRegistrations","networkmanager:GetTransitGatewayRouteTableAttachment","networkmanager:GetVpcAttachment","networkmanager:ListAttachmentRoutingPolicyAssociations","networkmanager:ListAttachments","networkmanager:ListConnectPeers","networkmanager:ListCoreNetworkPolicyVersions","networkmanager:ListCoreNetworkPrefixListAssociations","networkmanager:ListCoreNetworkRoutingInformation","networkmanager:ListCoreNetworks","networkmanager:ListPeerings","networkmanager:ListTagsForResource","networkmonitor:GetMonitor","networkmonitor:GetProbe","networkmonitor:ListMonitors","networkmonitor:ListTagsForResource","nimble:GetEula","nimble:GetFeatureMap","nimble:GetLaunchProfile","nimble:GetLaunchProfileDetails","nimble:GetLaunchProfileInitialization","nimble:GetLaunchProfileMember","nimble:GetStreamingImage","nimble:GetStreamingSession","nimble:GetStudio","nimble:GetStudioComponent","nimble:GetStudioMember","nimble:ListEulaAcceptances","nimble:ListEulas","nimble:ListLaunchProfileMembers","nimble:ListLaunchProfiles","nimble:ListStreamingImages","nimble:ListStreamingSessions","nimble:ListStudioComponents","nimble:ListStudioMembers","nimble:ListStudios","nimble:ListTagsForResource","notifications-contacts:GetEmailContact","notifications-contacts:ListEmailContacts","notifications-contacts:ListTagsForResource","notifications:GetEventRule","notifications:GetFeatureOptInStatus","notifications:GetManagedNotificationChildEvent","notifications:GetManagedNotificationConfiguration","notifications:GetManagedNotificationEvent","notifications:GetNotificationConfiguration","notifications:GetNotificationEvent","notifications:GetNotificationsAccessForOrganization","notifications:List*","oam:GetLink","oam:GetSink","oam:GetSinkPolicy","oam:ListAttachedLinks","oam:ListLinks","oam:ListSinks","observabilityadmin:GetCentralizationRuleForOrganization","observabilityadmin:GetS3TableIntegration","observabilityadmin:GetTelemetryEnrichmentStatus","observabilityadmin:GetTelemetryEvaluationStatus","observabilityadmin:GetTelemetryEvaluationStatusForOrganization","observabilityadmin:GetTelemetryPipeline","observabilityadmin:GetTelemetryRule","observabilityadmin:GetTelemetryRuleForOrganization","observabilityadmin:ListCentralizationRulesForOrganization","observabilityadmin:ListResourceTelemetry","observabilityadmin:ListResourceTelemetryForOrganization","observabilityadmin:ListS3TableIntegrations","observabilityadmin:ListTagsForResource","observabilityadmin:ListTelemetryPipelines","observabilityadmin:ListTelemetryRules","observabilityadmin:ListTelemetryRulesForOrganization","observabilityadmin:TestTelemetryPipeline","observabilityadmin:ValidateTelemetryPipelineConfiguration","omics:Get*","omics:List*","one:GetDeviceConfigurationTemplate","one:GetDeviceInstance","one:GetDeviceInstanceConfiguration","one:GetSite","one:GetSiteAddress","one:ListDeviceConfigurationTemplates","one:ListDeviceInstances","one:ListSites","one:ListUsers","opsworks-cm:Describe*","opsworks-cm:List*","opsworks:Describe*","opsworks:Get*","organizations:Describe*","organizations:List*","osis:GetPipeline","osis:GetPipelineBlueprint","osis:GetPipelineChangeProgress","osis:ListPipelineBlueprints","osis:ListPipelines","osis:ListTagsForResource","outposts:Get*","outposts:List*","payment-cryptography:GetAlias","payment-cryptography:GetKey","payment-cryptography:GetPublicKeyCertificate","payment-cryptography:ListAliases","payment-cryptography:ListKeys","payment-cryptography:ListTagsForResource","payments:GetPaymentInstrument","payments:GetPaymentStatus","payments:ListPaymentInstruments","payments:ListPaymentPreferences","payments:ListPaymentProgramOptions","payments:ListPaymentProgramStatus","payments:ListTagsForResource","pca-connector-ad:GetConnector","pca-connector-ad:GetDirectoryRegistration","pca-connector-ad:GetServicePrincipalName","pca-connector-ad:GetTemplate","pca-connector-ad:GetTemplateGroupAccessControlEntry","pca-connector-ad:ListConnectors","pca-connector-ad:ListDirectoryRegistrations","pca-connector-ad:ListServicePrincipalNames","pca-connector-ad:ListTagsForResource","pca-connector-ad:ListTemplateGroupAccessControlEntries","pca-connector-ad:ListTemplates","pca-connector-scep:GetChallengeMetadata","pca-connector-scep:GetConnector","pca-connector-scep:ListChallengeMetadata","pca-connector-scep:ListConnectors","pca-connector-scep:ListTagsForResource","pcs:GetCluster","pcs:GetComputeNodeGroup","pcs:GetQueue","pcs:ListClusters","pcs:ListComputeNodeGroups","pcs:ListQueues","pcs:ListTagsForResource","personalize:Describe*","personalize:Get*","personalize:List*","pi:DescribeDimensionKeys","pi:GetDimensionKeyDetails","pi:GetResourceMetadata","pi:GetResourceMetrics","pi:ListAvailableResourceDimensions","pi:ListAvailableResourceMetrics","pipes:DescribePipe","pipes:ListPipes","pipes:ListTagsForResource","polly:Describe*","polly:Get*","polly:List*","polly:SynthesizeSpeech","pricing:DescribeServices","pricing:GetAttributeValues","pricing:GetPriceListFileUrl","pricing:GetProducts","pricing:ListPriceLists","proton:GetDeployment","proton:GetEnvironment","proton:GetEnvironmentTemplate","proton:GetEnvironmentTemplateVersion","proton:GetService","proton:GetServiceInstance","proton:GetServiceTemplate","proton:GetServiceTemplateVersion","proton:ListDeployments","proton:ListEnvironmentAccountConnections","proton:ListEnvironments","proton:ListEnvironmentTemplates","proton:ListServiceInstances","proton:ListServices","proton:ListServiceTemplates","proton:ListTagsForResource","purchase-orders:GetPurchaseOrder","purchase-orders:ListPurchaseOrderInvoices","purchase-orders:ListPurchaseOrders","purchase-orders:ViewPurchaseOrders","qbusiness:GetApplication","qbusiness:GetChatControlsConfiguration","qbusiness:GetDataSource","qbusiness:GetGroup","qbusiness:GetIndex","qbusiness:GetPlugin","qbusiness:GetRetriever","qbusiness:GetUser","qbusiness:GetWebExperience","qbusiness:ListApplications","qbusiness:ListDataSources","qbusiness:ListDataSourceSyncJobs","qbusiness:ListGroups","qbusiness:ListIndices","qbusiness:ListPlugins","qbusiness:ListRetrievers","qbusiness:ListSubscriptions","qbusiness:ListTagsForResource","qbusiness:ListWebExperiences","qldb:DescribeJournalKinesisStream","qldb:DescribeJournalS3Export","qldb:DescribeLedger","qldb:GetBlock","qldb:GetDigest","qldb:GetRevision","qldb:ListJournalKinesisStreamsForLedger","qldb:ListJournalS3Exports","qldb:ListJournalS3ExportsForLedger","qldb:ListLedgers","qldb:ListTagsForResource","ram:Get*","ram:List*","rbin:GetRule","rbin:ListRules","rbin:ListTagsForResource","rds:Describe*","rds:Download*","rds:List*","redshift-serverless:GetCustomDomainAssociation","redshift-serverless:GetEndpointAccess","redshift-serverless:GetNamespace","redshift-serverless:GetRecoveryPoint","redshift-serverless:GetResourcePolicy","redshift-serverless:GetScheduledAction","redshift-serverless:GetSnapshot","redshift-serverless:GetTableRestoreStatus","redshift-serverless:GetUsageLimit","redshift-serverless:GetWorkgroup","redshift-serverless:ListCustomDomainAssociations","redshift-serverless:ListEndpointAccess","redshift-serverless:ListNamespaces","redshift-serverless:ListRecoveryPoints","redshift-serverless:ListScheduledActions","redshift-serverless:ListSnapshotCopyConfigurations","redshift-serverless:ListSnapshots","redshift-serverless:ListTableRestoreStatus","redshift-serverless:ListTagsForResource","redshift-serverless:ListUsageLimits","redshift-serverless:ListWorkgroups","redshift:Describe*","redshift:GetReservedNodeExchangeOfferings","redshift:ListRecommendations","redshift:View*","refactor-spaces:GetApplication","refactor-spaces:GetEnvironment","refactor-spaces:GetResourcePolicy","refactor-spaces:GetRoute","refactor-spaces:GetService","refactor-spaces:ListApplications","refactor-spaces:ListEnvironments","refactor-spaces:ListEnvironmentVpcs","refactor-spaces:ListRoutes","refactor-spaces:ListServices","refactor-spaces:ListTagsForResource","rekognition:CompareFaces","rekognition:DescribeDataset","rekognition:DescribeProjects","rekognition:DescribeProjectVersions","rekognition:DescribeStreamProcessor","rekognition:Detect*","rekognition:GetCelebrityInfo","rekognition:GetCelebrityRecognition","rekognition:GetContentModeration","rekognition:GetFaceDetection","rekognition:GetFaceSearch","rekognition:GetLabelDetection","rekognition:GetPersonTracking","rekognition:GetSegmentDetection","rekognition:GetTextDetection","rekognition:List*","rekognition:RecognizeCelebrities","rekognition:Search*","resiliencehub:DescribeApp","resiliencehub:DescribeAppAssessment","resiliencehub:DescribeAppVersion","resiliencehub:DescribeAppVersionAppComponent","resiliencehub:DescribeAppVersionResource","resiliencehub:DescribeAppVersionResourcesResolutionStatus","resiliencehub:DescribeAppVersionTemplate","resiliencehub:DescribeDraftAppVersionResourcesImportStatus","resiliencehub:DescribeMetricsExport","resiliencehub:DescribeResiliencyPolicy","resiliencehub:DescribeResourceGroupingRecommendationTask","resiliencehub:ListAlarmRecommendations","resiliencehub:ListAppAssessmentComplianceDrifts","resiliencehub:ListAppAssessmentResourceDrifts","resiliencehub:ListAppAssessments","resiliencehub:ListAppComponentCompliances","resiliencehub:ListAppComponentRecommendations","resiliencehub:ListAppInputSources","resiliencehub:ListApps","resiliencehub:ListAppVersionAppComponents","resiliencehub:ListAppVersionResourceMappings","resiliencehub:ListAppVersionResources","resiliencehub:ListAppVersions","resiliencehub:ListMetrics","resiliencehub:ListRecommendationTemplates","resiliencehub:ListResiliencyPolicies","resiliencehub:ListResourceGroupingRecommendations","resiliencehub:ListSopRecommendations","resiliencehub:ListSuggestedResiliencyPolicies","resiliencehub:ListTagsForResource","resiliencehub:ListTestRecommendations","resiliencehub:ListUnsupportedAppVersionResources","resource-explorer-2:BatchGetView","resource-explorer-2:GetAccountLevelServiceConfiguration","resource-explorer-2:GetDefaultView","resource-explorer-2:GetIndex","resource-explorer-2:GetManagedView","resource-explorer-2:GetResourceExplorerSetup","resource-explorer-2:GetServiceIndex","resource-explorer-2:GetServiceView","resource-explorer-2:GetView","resource-explorer-2:ListIndexes","resource-explorer-2:ListIndexesForMembers","resource-explorer-2:ListManagedViews","resource-explorer-2:ListServiceIndexes","resource-explorer-2:ListServiceViews","resource-explorer-2:ListStreamingAccessForServices","resource-explorer-2:ListSupportedResourceTypes","resource-explorer-2:ListTagsForResource","resource-explorer-2:ListViews","resource-explorer-2:Search","resource-groups:Get*","resource-groups:List*","resource-groups:Search*","robomaker:BatchDescribe*","robomaker:Describe*","robomaker:Get*","robomaker:List*","rolesanywhere:GetCrl","rolesanywhere:GetProfile","rolesanywhere:GetSubject","rolesanywhere:GetTrustAnchor","rolesanywhere:ListCrls","rolesanywhere:ListProfiles","rolesanywhere:ListSubjects","rolesanywhere:ListTagsForResource","rolesanywhere:ListTrustAnchors","route53-recovery-cluster:Get*","route53-recovery-cluster:ListRoutingControls","route53-recovery-control-config:Describe*","route53-recovery-control-config:GetResourcePolicy","route53-recovery-control-config:List*","route53-recovery-readiness:Get*","route53-recovery-readiness:List*","route53:Get*","route53:List*","route53:Test*","route53domains:Check*","route53domains:Get*","route53domains:List*","route53domains:View*","route53globalresolver:GetAccessSource","route53globalresolver:GetDNSView","route53globalresolver:GetFirewallDomainList","route53globalresolver:GetFirewallRule","route53globalresolver:GetGlobalResolver","route53globalresolver:GetHostedZoneAssociation","route53globalresolver:GetManagedFirewallDomainList","route53globalresolver:ListAccessSources","route53globalresolver:ListAccessTokens","route53globalresolver:ListDNSViews","route53globalresolver:ListFirewallDomainLists","route53globalresolver:ListFirewallDomains","route53globalresolver:ListFirewallRules","route53globalresolver:ListGlobalResolvers","route53globalresolver:ListHostedZoneAssociations","route53globalresolver:ListManagedFirewallDomainLists","route53profiles:GetProfile","route53profiles:GetProfileAssociation","route53profiles:GetProfileResourceAssociation","route53profiles:ListProfileAssociations","route53profiles:ListProfileResourceAssociations","route53profiles:ListProfiles","route53profiles:ListTagsForResource","route53resolver:Get*","route53resolver:List*","rum:GetAppMonitor","rum:GetAppMonitorData","rum:ListAppMonitors","s3-object-lambda:GetObject","s3-object-lambda:GetObjectAcl","s3-object-lambda:GetObjectLegalHold","s3-object-lambda:GetObjectRetention","s3-object-lambda:GetObjectTagging","s3-object-lambda:GetObjectVersion","s3-object-lambda:GetObjectVersionAcl","s3-object-lambda:GetObjectVersionTagging","s3-object-lambda:ListBucket","s3-object-lambda:ListBucketMultipartUploads","s3-object-lambda:ListBucketVersions","s3-object-lambda:ListMultipartUploadParts","s3-outposts:GetAccessPoint","s3-outposts:GetAccessPointPolicy","s3-outposts:GetBucket","s3-outposts:GetBucketPolicy","s3-outposts:GetBucketTagging","s3-outposts:GetBucketVersioning","s3-outposts:GetLifecycleConfiguration","s3-outposts:GetObject","s3-outposts:GetObjectTagging","s3-outposts:GetObjectVersion","s3-outposts:GetObjectVersionForReplication","s3-outposts:GetObjectVersionTagging","s3-outposts:GetReplicationConfiguration","s3-outposts:ListAccessPoints","s3-outposts:ListBucket","s3-outposts:ListBucketMultipartUploads","s3-outposts:ListBucketVersions","s3-outposts:ListEndpoints","s3-outposts:ListMultipartUploadParts","s3-outposts:ListOutpostsWithS3","s3-outposts:ListRegionalBuckets","s3-outposts:ListSharedEndpoints","s3:DescribeJob","s3:Get*","s3:List*","s3express:GetAccessPoint","s3express:GetAccessPointPolicy","s3express:GetAccessPointScope","s3express:GetBucketPolicy","s3express:GetEncryptionConfiguration","s3express:GetLifecycleConfiguration","s3express:ListAccessPointsForDirectoryBuckets","s3express:ListAllMyDirectoryBuckets","s3express:ListTagsForResource","s3tables:GetNamespace","s3tables:GetTable","s3tables:GetTableBucket","s3tables:GetTableBucketEncryption","s3tables:GetTableBucketMaintenanceConfiguration","s3tables:GetTableBucketPolicy","s3tables:GetTableBucketReplication","s3tables:GetTableBucketStorageClass","s3tables:GetTableData","s3tables:GetTableEncryption","s3tables:GetTableMaintenanceConfiguration","s3tables:GetTableMaintenanceJobStatus","s3tables:GetTableMetadataLocation","s3tables:GetTablePolicy","s3tables:GetTableRecordExpirationConfiguration","s3tables:GetTableRecordExpirationJobStatus","s3tables:GetTableReplication","s3tables:GetTableReplicationStatus","s3tables:GetTableStorageClass","s3tables:ListNamespaces","s3tables:ListTableBuckets","s3tables:ListTables","s3tables:ListTagsForResource","s3vectors:GetIndex","s3vectors:GetVectorBucket","s3vectors:GetVectorBucketPolicy","s3vectors:GetVectors","s3vectors:ListIndexes","s3vectors:ListVectorBuckets","s3vectors:ListVectors","s3vectors:QueryVectors","sagemaker:Describe*","sagemaker:GetSearchSuggestions","sagemaker:List*","sagemaker:Search","savingsplans:DescribeSavingsPlanRates","savingsplans:DescribeSavingsPlans","savingsplans:DescribeSavingsPlansOfferingRates","savingsplans:DescribeSavingsPlansOfferings","savingsplans:ListTagsForResource","scheduler:GetSchedule","scheduler:GetScheduleGroup","scheduler:ListScheduleGroups","scheduler:ListSchedules","scheduler:ListTagsForResource","schemas:Describe*","schemas:Get*","schemas:List*","schemas:Search*","sdb:Get*","sdb:List*","sdb:Select*","secretsmanager:Describe*","secretsmanager:GetResourcePolicy","secretsmanager:List*","securityagent:BatchGetAgentSpaces","securityagent:BatchGetTargetDomains","securityagent:BatchGetArtifactMetadata","securityagent:BatchGetFindings","securityagent:BatchGetPentestJobs","securityagent:BatchGetPentests","securityagent:BatchGetPentestJobContentMetadata","securityagent:BatchGetPentestJobTasks","securityagent:GetApplication","securityagent:GetArtifact","securityagent:GetDesignReview","securityagent:GetDesignReviewArtifact","securityagent:GetDesignReviewFeedback","securityagent:GetIntegration","securityagent:ListAgentSpaces","securityagent:ListTargetDomains","securityagent:ListApplications","securityagent:ListArtifacts","securityagent:ListSecurityRequirements","securityagent:ListDiscoveredEndpoints","securityagent:ListDesignReviewComments","securityagent:ListDesignReviews","securityagent:ListFindings","securityagent:ListIntegratedResources","securityagent:ListIntegrations","securityagent:ListMemberships","securityagent:ListPentestJobsForPentest","securityagent:ListPentests","securityagent:ListResourcesFromIntegration","securityagent:ListPentestJobTasks","securityhub:BatchGetAutomationRules","securityhub:BatchGetConfigurationPolicyAssociations","securityhub:BatchGetControlEvaluations","securityhub:BatchGetSecurityControls","securityhub:BatchGetStandardsControlAssociations","securityhub:Describe*","securityhub:Get*","securityhub:List*","securitylake:GetDataLakeExceptionSubscription","securitylake:GetDataLakeOrganizationConfiguration","securitylake:GetDataLakeSources","securitylake:GetSubscriber","securitylake:ListDataLakeExceptions","securitylake:ListDataLakes","securitylake:ListLogSources","securitylake:ListSubscribers","securitylake:ListTagsForResource","serverlessrepo:Get*","serverlessrepo:List*","serverlessrepo:SearchApplications","servicecatalog:Describe*","servicecatalog:GetApplication","servicecatalog:GetAttributeGroup","servicecatalog:List*","servicecatalog:Scan*","servicecatalog:Search*","servicediscovery:DiscoverInstances","servicediscovery:DiscoverInstancesRevision","servicediscovery:Get*","servicediscovery:List*","servicequotas:GetAssociationForServiceQuotaTemplate","servicequotas:GetAutoManagementConfiguration","servicequotas:GetAWSDefaultServiceQuota","servicequotas:GetQuotaUtilizationReport","servicequotas:GetRequestedServiceQuotaChange","servicequotas:GetServiceQuota","servicequotas:GetServiceQuotaIncreaseRequestFromTemplate","servicequotas:ListAWSDefaultServiceQuotas","servicequotas:ListRequestedServiceQuotaChangeHistory","servicequotas:ListRequestedServiceQuotaChangeHistoryByQuota","servicequotas:ListServiceQuotaIncreaseRequestsInTemplate","servicequotas:ListServiceQuotas","servicequotas:ListServices","servicequotas:StartQuotaUtilizationReport","ses:BatchGetMetricData","ses:Describe*","ses:Get*","ses:List*","shield:Describe*","shield:Get*","shield:List*","signer:DescribeSigningJob","signer:GetSigningPlatform","signer:GetSigningProfile","signer:ListProfilePermissions","signer:ListSigningJobs","signer:ListSigningPlatforms","signer:ListSigningProfiles","signer:ListTagsForResource","signin:ListTrustedIdentityPropagationApplicationsForConsole","sms-voice:DescribeAccountAttributes","sms-voice:DescribeAccountLimits","sms-voice:DescribeConfigurationSets","sms-voice:DescribeKeywords","sms-voice:DescribeOptedOutNumbers","sms-voice:DescribeOptOutLists","sms-voice:DescribePhoneNumbers","sms-voice:DescribePools","sms-voice:DescribeProtectConfigurations","sms-voice:DescribeRegistrationAttachments","sms-voice:DescribeRegistrationFieldDefinitions","sms-voice:DescribeRegistrationFieldValues","sms-voice:DescribeRegistrations","sms-voice:DescribeRegistrationSectionDefinitions","sms-voice:DescribeRegistrationTypeDefinitions","sms-voice:DescribeRegistrationVersions","sms-voice:DescribeSenderIds","sms-voice:DescribeSpendLimits","sms-voice:DescribeVerifiedDestinationNumbers","sms-voice:ListPoolOriginationIdentities","sms-voice:ListTagsForResource","snowball:Describe*","snowball:Get*","snowball:List*","sns:Check*","sns:Get*","sns:List*","sqs:Get*","sqs:List*","sqs:Receive*","ssm-contacts:DescribeEngagement","ssm-contacts:DescribePage","ssm-contacts:GetContact","ssm-contacts:GetContactChannel","ssm-contacts:ListContactChannels","ssm-contacts:ListContacts","ssm-contacts:ListEngagements","ssm-contacts:ListPageReceipts","ssm-contacts:ListPagesByContact","ssm-contacts:ListPagesByEngagement","ssm-incidents:GetIncidentRecord","ssm-incidents:GetReplicationSet","ssm-incidents:GetResourcePolicies","ssm-incidents:GetResponsePlan","ssm-incidents:GetTimelineEvent","ssm-incidents:ListIncidentRecords","ssm-incidents:ListRelatedItems","ssm-incidents:ListReplicationSets","ssm-incidents:ListResponsePlans","ssm-incidents:ListTagsForResource","ssm-incidents:ListTimelineEvents","ssm-quicksetup:GetConfiguration","ssm-quicksetup:GetConfigurationManager","ssm-quicksetup:GetServiceSettings","ssm-quicksetup:ListConfigurationManagers","ssm-quicksetup:ListConfigurations","ssm-quicksetup:ListQuickSetupTypes","ssm-quicksetup:ListTagsForResource","ssm-sap:GetApplication","ssm-sap:GetComponent","ssm-sap:GetConfigurationCheckOperation","ssm-sap:GetDatabase","ssm-sap:GetOperation","ssm-sap:GetResourcePermission","ssm-sap:ListApplications","ssm-sap:ListComponents","ssm-sap:ListConfigurationCheckDefinitions","ssm-sap:ListConfigurationCheckOperations","ssm-sap:ListDatabases","ssm-sap:ListOperationEvents","ssm-sap:ListOperations","ssm-sap:ListSubCheckResults","ssm-sap:ListSubCheckRuleResults","ssm-sap:ListTagsForResource","ssm:Describe*","ssm:Get*","ssm:List*","sso-directory:Describe*","sso-directory:List*","sso-directory:Search*","sso:Describe*","sso:Get*","sso:List*","states:Describe*","states:GetExecutionHistory","states:List*","states:ValidateStateMachineDefinition","storagegateway:Describe*","storagegateway:List*","sts:GetAccessKeyInfo","sts:GetCallerIdentity","sts:GetSessionToken","support:DescribeAttachment","support:DescribeCaseAttributes","support:DescribeCases","support:DescribeCommunication","support:DescribeCommunications","support:DescribeCreateCaseOptions","support:DescribeIssueTypes","support:DescribeServices","support:DescribeSeverityLevels","support:DescribeSupportedLanguages","support:DescribeSupportLevel","support:DescribeTrustedAdvisorCheckRefreshStatuses","support:DescribeTrustedAdvisorCheckResult","support:DescribeTrustedAdvisorChecks","support:DescribeTrustedAdvisorCheckSummaries","support:SearchForCases","supportplans:GetSupportPlan","supportplans:GetSupportPlanUpdateStatus","supportplans:ListSupportPlanModifiers","sustainability:GetCarbonFootprintSummary","swf:Count*","swf:Describe*","swf:Get*","swf:List*","synthetics:Describe*","synthetics:Get*","synthetics:List*","tag:DescribeReportCreation","tag:Get*","tax:GetExemptions","tax:GetTaxInheritance","tax:GetTaxInterview","tax:GetTaxRegistration","tax:GetTaxRegistrationDocument","tax:ListTaxRegistrations","timestream:DescribeBatchLoadTask","timestream:DescribeDatabase","timestream:DescribeEndpoints","timestream:DescribeTable","timestream:ListBatchLoadTasks","timestream:ListDatabases","timestream:ListMeasures","timestream:ListTables","timestream:ListTagsForResource","tnb:GetSolFunctionInstance","tnb:GetSolFunctionPackage","tnb:GetSolFunctionPackageContent","tnb:GetSolFunctionPackageDescriptor","tnb:GetSolNetworkInstance","tnb:GetSolNetworkOperation","tnb:GetSolNetworkPackage","tnb:GetSolNetworkPackageContent","tnb:GetSolNetworkPackageDescriptor","tnb:ListSolFunctionInstances","tnb:ListSolFunctionPackages","tnb:ListSolNetworkInstances","tnb:ListSolNetworkOperations","tnb:ListSolNetworkPackages","tnb:ListTagsForResource","transcribe:Get*","transcribe:List*","transfer:Describe*","transfer:List*","transfer:TestIdentityProvider","transform:GetAccountSettings","transform:GetAgent","transform:GetAgentRuntimeConfiguration","transform:GetConnector","transform:ListAgents","transform:ListConnectors","transform:ListProfiles","transform:ListTagsForResource","transform-custom:GetCampaign","transform-custom:GetKnowledgeItem","transform-custom:ListKnowledgeItems","transform-custom:ListTagsForResource","transform-custom:ListTransformationPackageMetadata","translate:DescribeTextTranslationJob","translate:GetParallelData","translate:GetTerminology","translate:ListParallelData","translate:ListTerminologies","translate:ListTextTranslationJobs","trustedadvisor:Describe*","trustedadvisor:GetOrganizationRecommendation","trustedadvisor:GetRecommendation","trustedadvisor:ListChecks","trustedadvisor:ListOrganizationRecommendationAccounts","trustedadvisor:ListOrganizationRecommendationResources","trustedadvisor:ListOrganizationRecommendations","trustedadvisor:ListRecommendationResources","trustedadvisor:ListRecommendations","user-subscriptions:ListApplicationClaims","user-subscriptions:ListClaims","user-subscriptions:ListUserSubscriptions","uxc:GetAccountColor","uxc:GetAccountCustomizations","uxc:ListServices","verifiedpermissions:GetIdentitySource","verifiedpermissions:GetPolicy","verifiedpermissions:GetPolicyStore","verifiedpermissions:GetPolicyTemplate","verifiedpermissions:GetSchema","verifiedpermissions:IsAuthorized","verifiedpermissions:IsAuthorizedWithToken","verifiedpermissions:ListIdentitySources","verifiedpermissions:ListPolicies","verifiedpermissions:ListPolicyStores","verifiedpermissions:ListPolicyTemplates","vpc-lattice:GetAccessLogSubscription","vpc-lattice:GetAuthPolicy","vpc-lattice:GetDomainVerification","vpc-lattice:GetListener","vpc-lattice:GetResourceConfiguration","vpc-lattice:GetResourceGateway","vpc-lattice:GetResourcePolicy","vpc-lattice:GetRule","vpc-lattice:GetService","vpc-lattice:GetServiceNetwork","vpc-lattice:GetServiceNetworkResourceAssociation","vpc-lattice:GetServiceNetworkServiceAssociation","vpc-lattice:GetServiceNetworkVpcAssociation","vpc-lattice:GetTargetGroup","vpc-lattice:ListAccessLogSubscriptions","vpc-lattice:ListDomainVerifications","vpc-lattice:ListListeners","vpc-lattice:ListResourceConfigurations","vpc-lattice:ListResourceEndpointAssociations","vpc-lattice:ListResourceGateways","vpc-lattice:ListRules","vpc-lattice:ListServiceNetworkResourceAssociations","vpc-lattice:ListServiceNetworks","vpc-lattice:ListServiceNetworkServiceAssociations","vpc-lattice:ListServiceNetworkVpcAssociations","vpc-lattice:ListServiceNetworkVpcEndpointAssociations","vpc-lattice:ListServices","vpc-lattice:ListTagsForResource","vpc-lattice:ListTargetGroups","vpc-lattice:ListTargets","waf-regional:Get*","waf-regional:List*","waf:Get*","waf:List*","wafv2:CheckCapacity","wafv2:Describe*","wafv2:Get*","wafv2:List*","wellarchitected:ExportLens","wellarchitected:GetAnswer","wellarchitected:GetConsolidatedReport","wellarchitected:GetLens","wellarchitected:GetLensReview","wellarchitected:GetLensReviewReport","wellarchitected:GetLensVersionDifference","wellarchitected:GetMilestone","wellarchitected:GetProfile","wellarchitected:GetProfileTemplate","wellarchitected:GetReviewTemplate","wellarchitected:GetReviewTemplateAnswer","wellarchitected:GetReviewTemplateLensReview","wellarchitected:GetWorkload","wellarchitected:List*","workdocs:CheckAlias","workdocs:Describe*","workdocs:Get*","workmail:Describe*","workmail:Get*","workmail:List*","workmail:Search*","workspaces-web:GetBrowserSettings","workspaces-web:GetIdentityProvider","workspaces-web:GetNetworkSettings","workspaces-web:GetPortal","workspaces-web:GetPortalServiceProviderMetadata","workspaces-web:GetTrustStore","workspaces-web:GetUserAccessLoggingSettings","workspaces-web:GetUserSettings","workspaces-web:ListBrowserSettings","workspaces-web:ListIdentityProviders","workspaces-web:ListNetworkSettings","workspaces-web:ListPortals","workspaces-web:ListTagsForResource","workspaces-web:ListTrustStores","workspaces-web:ListUserAccessLoggingSettings","workspaces-web:ListUserSettings","workspaces:Describe*","xray:BatchGet*","xray:CancelTraceRetrieval","xray:Get*","xray:ListResourcePolicies","xray:ListRetrievedTraces","xray:ListTagsForResource","xray:StartTraceRetrieval"],"Resource":"*"},{"Sid":"S3ExpressReadOnlySessionObjectAccess","Effect":"Allow","Action":["s3express:CreateSession"],"Resource":"*","Condition":{"StringEquals":{"s3express:SessionMode":"ReadOnly"}}}]}',
         "Provides read-only access to AWS services and resources."),
        ('SecurityAudit',
         '{"Version":"2012-10-17","Statement":[{"Sid":"BaseSecurityAuditStatement","Effect":"Allow","Action":["a4b:ListSkills","access-analyzer:GetAnalyzedResource","access-analyzer:GetAnalyzer","access-analyzer:GetArchiveRule","access-analyzer:GetFinding","access-analyzer:ListAnalyzedResources","access-analyzer:ListAnalyzers","access-analyzer:ListArchiveRules","access-analyzer:ListFindings","access-analyzer:ListTagsForResource","account:GetAccountInformation","account:GetAlternateContact","account:GetPrimaryEmail","account:GetRegionOptStatus","acm-pca:DescribeCertificateAuthority","acm-pca:DescribeCertificateAuthorityAuditReport","acm-pca:GetPolicy","acm-pca:ListCertificateAuthorities","acm-pca:ListPermissions","acm-pca:ListTags","acm:Describe*","acm:List*","airflow:GetEnvironment","airflow:ListEnvironments","appflow:ListFlows","appflow:ListTagsForResource","application-autoscaling:Describe*","appmesh:Describe*","appmesh:List*","apprunner:DescribeAutoScalingConfiguration","apprunner:DescribeCustomDomains","apprunner:DescribeObservabilityConfiguration","apprunner:DescribeService","apprunner:DescribeVpcConnector","apprunner:DescribeVpcIngressConnection","apprunner:ListAutoScalingConfigurations","apprunner:ListConnections","apprunner:ListObservabilityConfigurations","apprunner:ListOperations","apprunner:ListServices","apprunner:ListTagsForResource","apprunner:ListVpcConnectors","apprunner:ListVpcIngressConnections","appsync:GetApiCache","appsync:List*","athena:GetWorkGroup","athena:List*","auditmanager:GetAccountStatus","auditmanager:ListAssessmentControlInsightsByControlDomain","auditmanager:ListAssessmentFrameworks","auditmanager:ListAssessmentFrameworkShareRequests","auditmanager:ListAssessmentReports","auditmanager:ListAssessments","auditmanager:ListControlDomainInsights","auditmanager:ListControlDomainInsightsByAssessment","auditmanager:ListControlInsightsByControlDomain","auditmanager:ListControls","auditmanager:ListNotifications","auditmanager:ListTagsForResource","autoscaling-plans:DescribeScalingPlans","autoscaling:Describe*","backup:DescribeGlobalSettings","backup:DescribeRegionSettings","backup:GetBackupVaultAccessPolicy","backup:GetBackupVaultNotifications","backup:ListBackupVaults","backup:ListTags","batch:DescribeComputeEnvironments","batch:DescribeJobDefinitions","bedrock:GetAgentAlias","bedrock:GetAgentKnowledgeBase","bedrock:GetCustomModel","bedrock:GetFlowAlias","bedrock:GetFoundationModel","bedrock:GetFoundationModelAvailability","bedrock:GetImportedModel","bedrock:GetInferenceProfile","bedrock:GetIngestionJob","bedrock:GetKnowledgeBaseDocuments","bedrock:GetMarketplaceModelEndpoint","bedrock:GetModelCopyJob","bedrock:GetModelCustomizationJob","bedrock:GetModelImportJob","bedrock:GetModelInvocationLoggingConfiguration","bedrock:GetPromptRouter","bedrock:GetProvisionedModelThroughput","bedrock:ListAgentActionGroups","bedrock:ListAgentAliases","bedrock:ListAgentKnowledgeBases","bedrock:ListAgents","bedrock:ListAgentVersions","bedrock:ListCustomModels","bedrock:ListDataSources","bedrock:ListEvaluationJobs","bedrock:ListFlowAliases","bedrock:ListFlows","bedrock:ListFlowVersions","bedrock:ListFoundationModels","bedrock:ListGuardrails","bedrock:ListImportedModels","bedrock:ListInferenceProfiles","bedrock:ListIngestionJobs","bedrock:ListKnowledgeBases","bedrock:ListMarketplaceModelEndpoints","bedrock:ListModelCopyJobs","bedrock:ListModelCustomizationJobs","bedrock:ListModelImportJobs","bedrock:ListModelInvocationJobs","bedrock:ListPromptRouters","bedrock:ListPrompts","bedrock:ListProvisionedModelThroughputs","bedrock:ListTagsForResource","bedrock-agentcore:GetAgentRuntime","bedrock-agentcore:GetAgentRuntimeEndpoint","bedrock-agentcore:GetBrowser","bedrock-agentcore:GetBrowserProfile","bedrock-agentcore:GetCodeInterpreter","bedrock-agentcore:GetGateway","bedrock-agentcore:GetGatewayTarget","bedrock-agentcore:GetHarness","bedrock-agentcore:GetMemory","bedrock-agentcore:GetPolicy","bedrock-agentcore:GetPolicyEngine","bedrock-agentcore:GetPolicyGeneration","bedrock-agentcore:ListAgentRuntimeEndpoints","bedrock-agentcore:ListAgentRuntimeVersions","bedrock-agentcore:ListAgentRuntimes","bedrock-agentcore:ListBrowserProfiles","bedrock-agentcore:ListBrowsers","bedrock-agentcore:ListCodeInterpreters","bedrock-agentcore:ListGatewayTargets","bedrock-agentcore:ListGateways","bedrock-agentcore:ListHarnesses","bedrock-agentcore:ListMemories","bedrock-agentcore:ListPolicies","bedrock-agentcore:ListPolicyEngines","bedrock-agentcore:ListPolicyGenerationAssets","bedrock-agentcore:ListPolicyGenerations","braket:SearchJobs","braket:SearchQuantumTasks","chime:List*","cleanrooms:BatchGetCollaborationAnalysisTemplate","cleanrooms:BatchGetSchema","cleanrooms:BatchGetSchemaAnalysisRule","cleanrooms:GetAnalysisTemplate","cleanrooms:GetCollaboration","cleanrooms:GetCollaborationAnalysisTemplate","cleanrooms:GetCollaborationConfiguredAudienceModelAssociation","cleanrooms:GetCollaborationIdNamespaceAssociation","cleanrooms:GetCollaborationPrivacyBudgetTemplate","cleanrooms:GetConfiguredAudienceModelAssociation","cleanrooms:GetConfiguredTable","cleanrooms:GetConfiguredTableAnalysisRule","cleanrooms:GetConfiguredTableAssociation","cleanrooms:GetConfiguredTableAssociationAnalysisRule","cleanrooms:GetIdMappingTable","cleanrooms:GetIdNamespaceAssociation","cleanrooms:GetMembership","cleanrooms:GetPrivacyBudgetTemplate","cleanrooms:GetProtectedQuery","cleanrooms:GetSchema","cleanrooms:GetSchemaAnalysisRule","cleanrooms:ListAnalysisTemplates","cleanrooms:ListCollaborationAnalysisTemplates","cleanrooms:ListCollaborationConfiguredAudienceModelAssociations","cleanrooms:ListCollaborationIdNamespaceAssociations","cleanrooms:ListCollaborationPrivacyBudgets","cleanrooms:ListCollaborationPrivacyBudgetTemplates","cleanrooms:ListCollaborations","cleanrooms:ListConfiguredAudienceModelAssociations","cleanrooms:ListConfiguredTableAssociations","cleanrooms:ListConfiguredTables","cleanrooms:ListIdMappingTables","cleanrooms:ListIdNamespaceAssociations","cleanrooms:ListMembers","cleanrooms:ListMemberships","cleanrooms:ListPrivacyBudgets","cleanrooms:ListPrivacyBudgetTemplates","cleanrooms:ListProtectedQueries","cleanrooms:ListSchemas","cleanrooms:ListTagsForResource","cleanrooms:PreviewPrivacyImpact","cloud9:Describe*","cloud9:ListEnvironments","clouddirectory:ListDirectories","cloudformation:DescribeStack*","cloudformation:GetStackPolicy","cloudformation:GetTemplate","cloudformation:ListStack*","cloudfront:Get*","cloudfront:List*","cloudsearch:DescribeDomainEndpointOptions","cloudsearch:DescribeDomains","cloudsearch:DescribeServiceAccessPolicies","cloudtrail:DescribeTrails","cloudtrail:GetEventSelectors","cloudtrail:GetInsightSelectors","cloudtrail:GetTrail","cloudtrail:GetTrailStatus","cloudtrail:ListTags","cloudtrail:ListTrails","cloudtrail:LookupEvents","cloudwatch:Describe*","cloudwatch:GetDashboard","cloudwatch:ListDashboards","cloudwatch:ListTagsForResource","codeartifact:GetDomainPermissionsPolicy","codeartifact:GetRepositoryPermissionsPolicy","codeartifact:ListRepositories","codebuild:BatchGetProjects","codebuild:GetResourcePolicy","codebuild:ListProjects","codebuild:ListSourceCredentials","codecommit:BatchGetRepositories","codecommit:GetBranch","codecommit:GetObjectIdentifier","codecommit:GetRepository","codecommit:GetRepositoryTriggers","codecommit:List*","codedeploy:Batch*","codedeploy:Get*","codedeploy:List*","codepipeline:GetJobDetails","codepipeline:GetPipeline","codepipeline:GetPipelineExecution","codepipeline:GetPipelineState","codepipeline:ListPipelines","codestar:Describe*","codestar:List*","cognito-identity:Describe*","cognito-identity:GetIdentityPoolRoles","cognito-identity:ListIdentityPools","cognito-identity:ListTagsForResource","cognito-idp:Describe*","cognito-idp:ListDevices","cognito-idp:ListGroups","cognito-idp:ListIdentityProviders","cognito-idp:ListResourceServers","cognito-idp:ListTagsForResource","cognito-idp:ListUserImportJobs","cognito-idp:ListUserPoolClients","cognito-idp:ListUserPools","cognito-idp:ListUsers","cognito-idp:ListUsersInGroup","cognito-sync:Describe*","cognito-sync:List*","comprehend:Describe*","comprehend:List*","comprehendmedical:ListICD10CMInferenceJobs","comprehendmedical:ListPHIDetectionJobs","comprehendmedical:ListRxNormInferenceJobs","comprehendmedical:ListSNOMEDCTInferenceJobs","config:BatchGetAggregateResourceConfig","config:BatchGetResourceConfig","config:Deliver*","config:Describe*","config:Get*","config:List*","config:SelectAggregateResourceConfig","config:SelectResourceConfig","connect:ListApprovedOrigins","connect:ListInstanceAttributes","connect:ListInstances","connect:ListInstanceStorageConfigs","connect:ListIntegrationAssociations","connect:ListLambdaFunctions","connect:ListLexBots","connect:ListSecurityKeys","databrew:DescribeDataset","databrew:DescribeProject","databrew:ListJobs","databrew:ListProjects","dataexchange:ListDataSets","datapipeline:DescribeObjects","datapipeline:DescribePipelines","datapipeline:EvaluateExpression","datapipeline:GetPipelineDefinition","datapipeline:ListPipelines","datapipeline:QueryObjects","datapipeline:ValidatePipelineDefinition","datasync:Describe*","datasync:List*","dax:Describe*","dax:ListTags","deepracer:ListModels","detective:GetGraphIngestState","detective:ListGraphs","detective:ListMembers","devicefarm:ListProjects","directconnect:Describe*","discovery:DescribeAgents","discovery:DescribeConfigurations","discovery:DescribeContinuousExports","discovery:DescribeExportConfigurations","discovery:DescribeExportTasks","discovery:DescribeImportTasks","dms:Describe*","dms:ListTagsForResource","docdb-elastic:ListClusters","ds:DescribeDirectories","dynamodb:DescribeContinuousBackups","dynamodb:DescribeExport","dynamodb:DescribeGlobalTable","dynamodb:DescribeKinesisStreamingDestination","dynamodb:DescribeTable","dynamodb:DescribeTimeToLive","dynamodb:GetResourcePolicy","dynamodb:ListBackups","dynamodb:ListExports","dynamodb:ListGlobalTables","dynamodb:ListStreams","dynamodb:ListTables","dynamodb:ListTagsOfResource","ec2:Describe*","ec2:GetAllowedImagesSettings","ec2:GetEbsDefaultKmsKeyId","ec2:GetEbsEncryptionByDefault","ec2:GetImageBlockPublicAccessState","ec2:GetInstanceMetadataDefaults","ec2:GetManagedPrefixListAssociations","ec2:GetManagedPrefixListEntries","ec2:GetNetworkInsightsAccessScopeAnalysisFindings","ec2:GetNetworkInsightsAccessScopeContent","ec2:GetSerialConsoleAccessStatus","ec2:GetSnapshotBlockPublicAccessState","ec2:GetTransitGatewayAttachmentPropagations","ec2:GetTransitGatewayMulticastDomainAssociations","ec2:GetTransitGatewayPrefixListReferences","ec2:GetTransitGatewayPrefixListReferences","ec2:GetTransitGatewayRouteTableAssociations","ec2:GetTransitGatewayRouteTablePropagations","ec2:SearchTransitGatewayRoutes","ec2:SearchTransitGatewayRoutes","ecr-public:DescribeImages","ecr-public:DescribeImageTags","ecr-public:DescribeRegistries","ecr-public:DescribeRepositories","ecr-public:GetRegistryCatalogData","ecr-public:GetRepositoryCatalogData","ecr-public:GetRepositoryPolicy","ecr-public:ListTagsForResource","ecr:BatchGetRepositoryScanningConfiguration","ecr:DescribeImages","ecr:DescribeImageScanFindings","ecr:DescribeRegistry","ecr:DescribeRepositories","ecr:GetLifecyclePolicy","ecr:GetRegistryPolicy","ecr:GetRegistryScanningConfiguration","ecr:GetRepositoryPolicy","ecr:ListImages","ecr:ListTagsForResource","ecs:Describe*","ecs:List*","eks:DescribeCluster","eks:DescribeFargateProfile","eks:DescribeNodeGroup","eks:ListAccessEntries","eks:ListAssociatedAccessPolicies","eks:ListClusters","eks:ListFargateProfiles","eks:ListNodeGroups","eks:ListTagsForResource","eks:ListUpdates","elasticache:Describe*","elasticache:ListTagsForResource","elasticbeanstalk:Describe*","elasticbeanstalk:ListTagsForResource","elasticfilesystem:DescribeAccessPoints","elasticfilesystem:DescribeAccountPreferences","elasticfilesystem:DescribeBackupPolicy","elasticfilesystem:DescribeFileSystemPolicy","elasticfilesystem:DescribeFileSystems","elasticfilesystem:DescribeLifecycleConfiguration","elasticfilesystem:DescribeMountTargets","elasticfilesystem:DescribeMountTargetSecurityGroups","elasticfilesystem:DescribeReplicationConfigurations","elasticfilesystem:DescribeTags","elasticloadbalancing:Describe*","elasticmapreduce:Describe*","elasticmapreduce:GetAutoTerminationPolicy","elasticmapreduce:GetBlockPublicAccessConfiguration","elasticmapreduce:GetManagedScalingPolicy","elasticmapreduce:ListClusters","elasticmapreduce:ListInstances","elasticmapreduce:ListSecurityConfigurations","elastictranscoder:ListPipelines","emr-serverless:GetApplication","emr-serverless:ListApplications","emr-serverless:ListJobRuns","entityresolution:GetIdNamespace","es:Describe*","es:GetCompatibleVersions","es:ListDomainNames","es:ListElasticsearchInstanceTypeDetails","es:ListElasticsearchVersions","es:ListTags","events:Describe*","events:List*","events:TestEventPattern","finspace:ListEnvironments","finspace:ListKxEnvironments","firehose:Describe*","firehose:List*","fms:ListComplianceStatus","fms:ListPolicies","forecast:ListDatasets","frauddetector:GetDetectors","fsx:Describe*","fsx:List*","gamelift:ListBuilds","gamelift:ListFleets","geo:ListMaps","glacier:DescribeVault","glacier:GetDataRetrievalPolicy","glacier:GetVaultAccessPolicy","glacier:GetVaultLock","glacier:ListVaults","globalaccelerator:Describe*","globalaccelerator:List*","glue:GetCrawlers","glue:GetDatabases","glue:GetDataCatalogEncryptionSettings","glue:GetDevEndpoints","glue:GetJobs","glue:GetResourcePolicy","glue:GetSecurityConfiguration","glue:GetSecurityConfigurations","glue:GetTags","grafana:ListWorkspaces","greengrass:List*","guardduty:DescribeMalwareScans","guardduty:DescribeOrganizationConfiguration","guardduty:DescribePublishingDestination","guardduty:Get*","guardduty:List*","health:DescribeAffectedAccountsForOrganization","health:DescribeAffectedEntities","health:DescribeAffectedEntitiesForOrganization","health:DescribeEntityAggregates","health:DescribeEventAggregates","health:DescribeEventDetails","health:DescribeEventDetailsForOrganization","health:DescribeEvents","health:DescribeEventsForOrganization","health:DescribeEventTypes","health:DescribeHealthServiceStatusForOrganization","healthlake:ListFHIRDatastores","honeycode:ListTables","iam:GenerateCredentialReport","iam:GenerateServiceLastAccessedDetails","iam:Get*","iam:List*","iam:SimulateCustomPolicy","iam:SimulatePrincipalPolicy","identitystore:DescribeGroupMembership","identitystore:GetGroupId","identitystore:GetGroupMembershipId","identitystore:GetUserId","identitystore:IsMemberInGroups","identitystore:ListGroupMemberships","identitystore:ListGroupMembershipsForMember","identitystore:ListGroups","identitystore:ListUsers","inspector:Describe*","inspector:Get*","inspector:List*","inspector:Preview*","inspector2:BatchGetAccountStatus","inspector2:BatchGetFreeTrialInfo","inspector2:DescribeOrganizationConfiguration","inspector2:GetConfiguration","inspector2:GetDelegatedAdminAccount","inspector2:GetFindingsReportStatus","inspector2:GetMember","inspector2:ListAccountPermissions","inspector2:ListCoverage","inspector2:ListCoverageStatistics","inspector2:ListDelegatedAdminAccounts","inspector2:ListFilters","inspector2:ListFindingAggregations","inspector2:ListFindings","inspector2:ListTagsForResource","inspector2:ListUsageTotals","iot:Describe*","iot:GetPolicy","iot:GetPolicyVersion","iot:List*","iotanalytics:ListChannels","iotevents:ListInputs","iotfleetwise:ListModelManifests","iotsitewise:DescribeGatewayCapabilityConfiguration","iotsitewise:ListAssetModels","iotsitewise:ListGateways","iottwinmaker:ListWorkspaces","kafka-cluster:Describe*","kafka:Describe*","kafka:GetBootstrapBrokers","kafka:GetCompatibleKafkaVersions","kafka:List*","kafkaconnect:Describe*","kafkaconnect:List*","kendra:DescribeIndex","kendra:ListDataSources","kendra:ListIndices","kendra:ListTagsForResource","kinesis:DescribeLimits","kinesis:DescribeStream","kinesis:DescribeStreamConsumer","kinesis:DescribeStreamSummary","kinesis:ListShards","kinesis:ListStreamConsumers","kinesis:ListStreams","kinesis:ListTagsForStream","kinesisanalytics:ListApplications","kinesisanalytics:ListTagsForResource","kinesisvideo:DescribeEdgeConfiguration","kinesisvideo:DescribeMappedResourceConfiguration","kinesisvideo:DescribeMediaStorageConfiguration","kinesisvideo:DescribeNotificationConfiguration","kinesisvideo:DescribeSignalingChannel","kinesisvideo:DescribeStream","kinesisvideo:ListSignalingChannels","kinesisvideo:ListStreams","kinesisvideo:ListTagsForResource","kinesisvideo:ListTagsForStream","kms:Describe*","kms:Get*","kms:List*","lambda:GetAccountSettings","lambda:GetFunctionCodeSigningConfig","lambda:GetFunctionConcurrency","lambda:GetFunctionConfiguration","lambda:GetFunctionEventInvokeConfig","lambda:GetLayerVersionPolicy","lambda:GetPolicy","lambda:GetRuntimeManagementConfig","lambda:List*","lex:DescribeBot","lex:DescribeResourcePolicy","lex:ListBots","license-manager:List*","lightsail:GetBuckets","lightsail:GetContainerServices","lightsail:GetDisks","lightsail:GetDiskSnapshots","lightsail:GetInstances","lightsail:GetLoadBalancers","logs:Describe*","logs:GetLogDelivery","logs:ListLogDeliveries","logs:ListTagsForResource","logs:ListTagsLogGroup","lookoutequipment:ListDatasets","lookoutmetrics:ListAnomalyDetectors","lookoutvision:ListProjects","m2:GetApplication","m2:GetEnvironment","m2:ListApplications","m2:ListEnvironments","m2:ListTagsForResource","machinelearning:DescribeMLModels","macie2:ListFindings","managedblockchain:ListNetworks","mechanicalturk:ListHITs","mediaconnect:Describe*","mediaconnect:List*","medialive:ListChannels","mediapackage-vod:DescribePackagingGroup","mediapackage-vod:ListPackagingGroups","mediapackage:DescribeOriginEndpoint","mediapackage:ListOriginEndpoints","mediastore:GetContainerPolicy","mediastore:GetCorsPolicy","mediastore:ListContainers","memorydb:DescribeClusters","mq:DescribeBroker","mq:DescribeBrokerEngineTypes","mq:DescribeBrokerInstanceOptions","mq:DescribeConfiguration","mq:DescribeConfigurationRevision","mq:DescribeUser","mq:ListBrokers","mq:ListConfigurationRevisions","mq:ListConfigurations","mq:ListTags","mq:ListUsers","network-firewall:DescribeFirewall","network-firewall:DescribeFirewallPolicy","network-firewall:DescribeLoggingConfiguration","network-firewall:DescribeResourcePolicy","network-firewall:DescribeRuleGroup","network-firewall:ListFirewallPolicies","network-firewall:ListFirewalls","network-firewall:ListRuleGroups","networkmanager:DescribeGlobalNetworks","nimble:ListStudios","opsworks-cm:DescribeServers","opsworks:DescribeStacks","organizations:Describe*","organizations:List*","pcs:GetCluster","pcs:GetComputeNodeGroup","pcs:GetQueue","pcs:ListClusters","pcs:ListComputeNodeGroups","pcs:ListQueues","pcs:ListTagsForResource","personalize:DescribeDatasetGroup","personalize:ListDatasetGroups","private-networks:ListNetworks","profile:GetDomain","profile:ListDomains","profile:ListIntegrations","qbusiness:ListApplications","qbusiness:ListDataSources","qbusiness:ListDataSourceSyncJobs","qbusiness:ListDocuments","qbusiness:ListGroups","qbusiness:ListIndices","qbusiness:ListPlugins","qbusiness:ListRetrievers","qbusiness:ListSubscriptions","qbusiness:ListTagsForResource","qbusiness:ListWebExperiences","qldb:DescribeJournalS3Export","qldb:DescribeLedger","qldb:ListJournalS3Exports","qldb:ListJournalS3ExportsForLedger","qldb:ListLedgers","quicksight:Describe*","quicksight:List*","ram:GetResourceShares","ram:List*","rds:Describe*","rds:DownloadDBLogFilePortion","rds:ListTagsForResource","redshift-serverless:GetNamespace","redshift-serverless:ListTagsForResource","redshift-serverless:ListWorkgroups","redshift:Describe*","rekognition:Describe*","rekognition:List*","resource-groups:ListGroupResources","robomaker:Describe*","robomaker:List*","rolesanywhere:GetCrl","rolesanywhere:GetProfile","rolesanywhere:GetSubject","rolesanywhere:GetTrustAnchor","rolesanywhere:ListCrls","rolesanywhere:ListProfiles","rolesanywhere:ListSubjects","rolesanywhere:ListTagsForResource","rolesanywhere:ListTrustAnchors","route53:Get*","route53:List*","route53domains:GetDomainDetail","route53domains:GetOperationDetail","route53domains:ListDomains","route53domains:ListOperations","route53domains:ListTagsForDomain","route53resolver:Get*","route53resolver:List*","s3-object-lambda:GetObjectAcl","s3-object-lambda:GetObjectVersionAcl","s3-outposts:ListEndpoints","s3-outposts:ListOutpostsWithS3","s3-outposts:ListSharedEndpoints","s3:DescribeJob","s3:GetAccelerateConfiguration","s3:GetAccessGrantsInstanceResourcePolicy","s3:GetAccessPoint","s3:GetAccessPointConfigurationForObjectLambda","s3:GetAccessPointForObjectLambda","s3:GetAccessPointPolicy","s3:GetAccessPointPolicyForObjectLambda","s3:GetAccessPointPolicyStatus","s3:GetAccessPointPolicyStatusForObjectLambda","s3:GetAccountPublicAccessBlock","s3:GetAnalyticsConfiguration","s3:GetBucket*","s3:GetEncryptionConfiguration","s3:GetInventoryConfiguration","s3:GetLifecycleConfiguration","s3:GetMetricsConfiguration","s3:GetMultiRegionAccessPoint","s3:GetMultiRegionAccessPointPolicy","s3:GetMultiRegionAccessPointPolicyStatus","s3:GetObjectAcl","s3:GetObjectTagging","s3:GetObjectVersionAcl","s3:GetReplicationConfiguration","s3:GetStorageLensConfiguration","s3:GetStorageLensGroup","s3:ListAccessGrants","s3:ListAccessGrantsInstances","s3:ListAccessPoints","s3:ListAccessPointsForObjectLambda","s3:ListAllMyBuckets","s3:ListBucket","s3:ListCallerAccessGrants","s3:ListJobs","s3:ListMultiRegionAccessPoints","s3:ListStorageLensConfigurations","s3:ListStorageLensGroups","s3express:GetBucketPolicy","s3express:GetEncryptionConfiguration","s3express:ListAllMyDirectoryBuckets","s3tables:GetNamespace","s3tables:GetTableBucketMaintenanceConfiguration","s3tables:GetTableBucketPolicy","s3tables:GetTableMaintenanceConfiguration","s3tables:GetTablePolicy","s3tables:ListNamespaces","s3tables:ListTableBuckets","s3tables:ListTables","sagemaker:Describe*","sagemaker:List*","schemas:DescribeCodeBinding","schemas:DescribeDiscoverer","schemas:DescribeRegistry","schemas:DescribeSchema","schemas:GetResourcePolicy","schemas:ListDiscoverers","schemas:ListRegistries","schemas:ListSchemas","schemas:ListSchemaVersions","schemas:ListTagsForResource","sdb:DomainMetadata","sdb:ListDomains","secretsmanager:DescribeSecret","secretsmanager:GetResourcePolicy","secretsmanager:ListSecrets","secretsmanager:ListSecretVersionIds","securityhub:BatchGetAutomationRules","securityhub:BatchGetConfigurationPolicyAssociations","securityhub:BatchGetControlEvaluations","securityhub:BatchGetSecurityControls","securityhub:BatchGetStandardsControlAssociations","securityhub:Describe*","securityhub:Get*","securityhub:List*","serverlessrepo:GetApplicationPolicy","serverlessrepo:List*","servicequotas:GetAssociationForServiceQuotaTemplate","servicequotas:GetAWSDefaultServiceQuota","servicequotas:GetRequestedServiceQuotaChange","servicequotas:GetServiceQuota","servicequotas:GetServiceQuotaIncreaseRequestFromTemplate","servicequotas:ListAWSDefaultServiceQuotas","servicequotas:ListRequestedServiceQuotaChangeHistory","servicequotas:ListRequestedServiceQuotaChangeHistoryByQuota","servicequotas:ListServiceQuotaIncreaseRequestsInTemplate","servicequotas:ListServiceQuotas","servicequotas:ListServices","servicequotas:ListTagsForResource","ses:Describe*","ses:GetAccount","ses:GetAccountSendingEnabled","ses:GetConfigurationSet","ses:GetConfigurationSetEventDestinations","ses:GetDedicatedIps","ses:GetEmailIdentity","ses:GetIdentityDkimAttributes","ses:GetIdentityPolicies","ses:GetIdentityVerificationAttributes","ses:ListConfigurationSets","ses:ListDedicatedIpPools","ses:ListIdentities","ses:ListIdentityPolicies","ses:ListReceiptFilters","ses:ListReceiptRuleSets","ses:ListVerifiedEmailAddresses","shield:Describe*","shield:GetSubscriptionState","shield:List*","snowball:ListClusters","snowball:ListJobs","sns:GetPlatformApplicationAttributes","sns:GetTopicAttributes","sns:ListSubscriptions","sns:ListSubscriptionsByTopic","sns:ListTagsForResource","sns:ListTopics","sqs:GetQueueAttributes","sqs:ListDeadLetterSourceQueues","sqs:ListQueues","sqs:ListQueueTags","ssm:Describe*","ssm:GetAutomationExecution","ssm:GetServiceSetting","ssm:ListAssociations","ssm:ListAssociationVersions","ssm:ListCommands","ssm:ListComplianceItems","ssm:ListComplianceSummaries","ssm:ListDocumentMetadataHistory","ssm:ListDocuments","ssm:ListDocumentVersions","ssm:ListInventoryEntries","ssm:ListOpsMetadata","ssm:ListResourceComplianceSummaries","ssm:ListResourceDataSync","ssm:ListTagsForResource","sso:DescribeAccountAssignmentCreationStatus","sso:DescribeAccountAssignmentDeletionStatus","sso:DescribeApplication","sso:DescribeApplicationAssignment","sso:DescribeApplicationProvider","sso:DescribeInstance","sso:DescribeInstanceAccessControlAttributeConfiguration","sso:DescribePermissionSet","sso:DescribePermissionSetProvisioningStatus","sso:DescribeRegion","sso:DescribeTrustedTokenIssuer","sso:GetApplicationAccessScope","sso:GetApplicationAssignmentConfiguration","sso:GetApplicationAuthenticationMethod","sso:GetApplicationGrant","sso:GetApplicationSessionConfiguration","sso:GetInlinePolicyForPermissionSet","sso:GetPermissionsBoundaryForPermissionSet","sso:ListAccountAssignmentCreationStatus","sso:ListAccountAssignmentDeletionStatus","sso:ListAccountAssignments","sso:ListAccountAssignmentsForPrincipal","sso:ListAccountsForProvisionedPermissionSet","sso:ListApplicationAccessScopes","sso:ListApplicationAssignments","sso:ListApplicationAssignmentsForPrincipal","sso:ListApplicationAuthenticationMethods","sso:ListApplicationGrants","sso:ListApplicationInstanceCertificates","sso:ListApplicationInstances","sso:ListApplicationProviders","sso:ListApplications","sso:ListApplicationTemplates","sso:ListCustomerManagedPolicyReferencesInPermissionSet","sso:ListDirectoryAssociations","sso:ListInstances","sso:ListManagedPoliciesInPermissionSet","sso:ListPermissionSetProvisioningStatus","sso:ListPermissionSets","sso:ListPermissionSetsProvisionedToAccount","sso:ListProfileAssociations","sso:ListProfiles","sso:ListRegions","sso:ListTagsForResource","sso:ListTrustedTokenIssuers","sso-directory:ListExternalIdPConfigurationsForDirectory","states:DescribeStateMachine","states:ListStateMachines","storagegateway:DescribeBandwidthRateLimit","storagegateway:DescribeCache","storagegateway:DescribeCachediSCSIVolumes","storagegateway:DescribeGatewayInformation","storagegateway:DescribeMaintenanceStartTime","storagegateway:DescribeNFSFileShares","storagegateway:DescribeSnapshotSchedule","storagegateway:DescribeStorediSCSIVolumes","storagegateway:DescribeTapeArchives","storagegateway:DescribeTapeRecoveryPoints","storagegateway:DescribeTapes","storagegateway:DescribeUploadBuffer","storagegateway:DescribeVTLDevices","storagegateway:DescribeWorkingStorage","storagegateway:List*","sts:GetAccessKeyInfo","support:DescribeTrustedAdvisorCheckRefreshStatuses","support:DescribeTrustedAdvisorCheckResult","support:DescribeTrustedAdvisorChecks","support:DescribeTrustedAdvisorCheckSummaries","synthetics:DescribeCanaries","synthetics:DescribeCanariesLastRun","synthetics:DescribeRuntimeVersions","synthetics:GetCanary","synthetics:GetCanaryRuns","synthetics:GetGroup","synthetics:ListAssociatedGroups","synthetics:ListGroupResources","synthetics:ListGroups","synthetics:ListTagsForResource","tag:GetResources","tag:GetTagKeys","transcribe:GetCallAnalyticsCategory","transcribe:GetMedicalVocabulary","transcribe:GetVocabulary","transcribe:GetVocabularyFilter","transcribe:ListCallAnalyticsCategories","transcribe:ListCallAnalyticsJobs","transcribe:ListLanguageModels","transcribe:ListMedicalTranscriptionJobs","transcribe:ListMedicalVocabularies","transcribe:ListTagsForResource","transcribe:ListTranscriptionJobs","transcribe:ListVocabularies","transcribe:ListVocabularyFilters","transfer:Describe*","transfer:List*","translate:List*","trustedadvisor:Describe*","voiceid:DescribeDomain","waf-regional:GetWebACL","waf-regional:ListResourcesForWebACL","waf-regional:ListTagsForResource","waf-regional:ListWebACLs","waf:GetWebACL","waf:ListTagsForResource","waf:ListWebACLs","wafv2:GetLoggingConfiguration","wafv2:GetWebACL","wafv2:GetWebACLForResource","wafv2:ListAvailableManagedRuleGroups","wafv2:ListIPSets","wafv2:ListLoggingConfigurations","wafv2:ListRegexPatternSets","wafv2:ListResourcesForWebACL","wafv2:ListRuleGroups","wafv2:ListTagsForResource","wafv2:ListWebACLs","wisdom:GetAssistant","workdocs:DescribeResourcePermissions","workspaces:Describe*","xray:GetEncryptionConfig","xray:GetGroup","xray:GetGroups","xray:GetSamplingRules","xray:GetSamplingTargets","xray:GetTraceSummaries","xray:ListTagsForResource"],"Resource":"*"},{"Sid":"APIGatewayAccess","Effect":"Allow","Action":["apigateway:GET"],"Resource":["arn:aws:apigateway:*::/apis","arn:aws:apigateway:*::/apis/*/authorizers/*","arn:aws:apigateway:*::/apis/*/authorizers","arn:aws:apigateway:*::/apis/*/cors","arn:aws:apigateway:*::/apis/*/deployments/*","arn:aws:apigateway:*::/apis/*/deployments","arn:aws:apigateway:*::/apis/*/exports/*","arn:aws:apigateway:*::/apis/*/integrations/*","arn:aws:apigateway:*::/apis/*/integrations","arn:aws:apigateway:*::/apis/*/models/*","arn:aws:apigateway:*::/apis/*/models","arn:aws:apigateway:*::/apis/*/routes/*","arn:aws:apigateway:*::/apis/*/routes","arn:aws:apigateway:*::/apis/*/stages","arn:aws:apigateway:*::/apis/*/stages/*","arn:aws:apigateway:*::/clientcertificates","arn:aws:apigateway:*::/clientcertificates/*","arn:aws:apigateway:*::/domainnames","arn:aws:apigateway:*::/domainnames/*/apimappings","arn:aws:apigateway:*::/restapis","arn:aws:apigateway:*::/restapis/*/authorizers/*","arn:aws:apigateway:*::/restapis/*/authorizers","arn:aws:apigateway:*::/restapis/*/deployments/*","arn:aws:apigateway:*::/restapis/*/deployments","arn:aws:apigateway:*::/restapis/*/documentation/parts/*","arn:aws:apigateway:*::/restapis/*/documentation/parts","arn:aws:apigateway:*::/restapis/*/documentation/versions/*","arn:aws:apigateway:*::/restapis/*/documentation/versions","arn:aws:apigateway:*::/restapis/*/gatewayresponses/*","arn:aws:apigateway:*::/restapis/*/gatewayresponses","arn:aws:apigateway:*::/restapis/*/models/*","arn:aws:apigateway:*::/restapis/*/models","arn:aws:apigateway:*::/restapis/*/requestvalidators","arn:aws:apigateway:*::/restapis/*/requestvalidators/*","arn:aws:apigateway:*::/restapis/*/resources/*","arn:aws:apigateway:*::/restapis/*/resources","arn:aws:apigateway:*::/restapis/*/stages","arn:aws:apigateway:*::/restapis/*/stages/*","arn:aws:apigateway:*::/tags/*","arn:aws:apigateway:*::/vpclinks"]}]}',
         "The security audit template grants access to read security configuration metadata. It is useful for software that audits the configuration of an AWS account."),
        ('IAMFullAccess',
         '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["iam:*","organizations:DescribeAccount","organizations:DescribeOrganization","organizations:DescribeOrganizationalUnit","organizations:DescribePolicy","organizations:ListChildren","organizations:ListParents","organizations:ListPoliciesForTarget","organizations:ListRoots","organizations:ListPolicies","organizations:ListTargetsForPolicy"],"Resource":"*"}]}',
         "Provides full access to IAM via the AWS Management Console."),
        ('IAMReadOnlyAccess',
         '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["iam:GenerateCredentialReport","iam:GenerateServiceLastAccessedDetails","iam:Get*","iam:List*","iam:SimulateCustomPolicy","iam:SimulatePrincipalPolicy"],"Resource":"*"}]}',
         "Provides read only access to IAM via the AWS Management Console."),
        ('AmazonS3FullAccess',
         '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:*","s3-object-lambda:*"],"Resource":"*"}]}',
         "Provides full access to all buckets via the AWS Management Console."),
        ('AmazonS3ReadOnlyAccess',
         '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:Get*","s3:List*","s3:Describe*","s3-object-lambda:Get*","s3-object-lambda:List*"],"Resource":"*"}]}',
         "Provides read only access to all buckets via the AWS Management Console."),
        ('AmazonEC2FullAccess',
         '{"Version":"2012-10-17","Statement":[{"Action":"ec2:*","Effect":"Allow","Resource":"*"},{"Effect":"Allow","Action":"elasticloadbalancing:*","Resource":"*"},{"Effect":"Allow","Action":"cloudwatch:*","Resource":"*"},{"Effect":"Allow","Action":"autoscaling:*","Resource":"*"},{"Effect":"Allow","Action":"iam:CreateServiceLinkedRole","Resource":"*","Condition":{"StringEquals":{"iam:AWSServiceName":["autoscaling.amazonaws.com","ec2scheduled.amazonaws.com","elasticloadbalancing.amazonaws.com","spot.amazonaws.com","spotfleet.amazonaws.com","transitgateway.amazonaws.com"]}}}]}',
         "Provides full access to Amazon EC2 via the AWS Management Console."),
        ('AmazonEC2ReadOnlyAccess',
         '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["ec2:Describe*","ec2:GetSecurityGroupsForVpc"],"Resource":"*"},{"Effect":"Allow","Action":"elasticloadbalancing:Describe*","Resource":"*"},{"Effect":"Allow","Action":["cloudwatch:ListMetrics","cloudwatch:GetMetricStatistics","cloudwatch:Describe*"],"Resource":"*"},{"Effect":"Allow","Action":"autoscaling:Describe*","Resource":"*"}]}',
         "Provides read only access to Amazon EC2 via the AWS Management Console."),
        ('AmazonSSMManagedInstanceCore',
         '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["ssm:DescribeAssociation","ssm:GetDeployablePatchSnapshotForInstance","ssm:GetDocument","ssm:DescribeDocument","ssm:GetManifest","ssm:GetParameter","ssm:GetParameters","ssm:ListAssociations","ssm:ListInstanceAssociations","ssm:PutInventory","ssm:PutComplianceItems","ssm:PutConfigurePackageResult","ssm:UpdateAssociationStatus","ssm:UpdateInstanceAssociationStatus","ssm:UpdateInstanceInformation"],"Resource":"*"},{"Effect":"Allow","Action":["ssmmessages:CreateControlChannel","ssmmessages:CreateDataChannel","ssmmessages:OpenControlChannel","ssmmessages:OpenDataChannel"],"Resource":"*"},{"Effect":"Allow","Action":["ec2messages:AcknowledgeMessage","ec2messages:DeleteMessage","ec2messages:FailMessage","ec2messages:GetEndpoint","ec2messages:GetMessages","ec2messages:SendReply"],"Resource":"*"}]}',
         "The policy for Amazon EC2 Role to enable AWS Systems Manager service core functionality."),
        ('AmazonDynamoDBFullAccess',
         '{"Version":"2012-10-17","Statement":[{"Action":["dynamodb:*","dax:*","application-autoscaling:DeleteScalingPolicy","application-autoscaling:DeregisterScalableTarget","application-autoscaling:DescribeScalableTargets","application-autoscaling:DescribeScalingActivities","application-autoscaling:DescribeScalingPolicies","application-autoscaling:PutScalingPolicy","application-autoscaling:RegisterScalableTarget","cloudwatch:DeleteAlarms","cloudwatch:DescribeAlarmHistory","cloudwatch:DescribeAlarms","cloudwatch:DescribeAlarmsForMetric","cloudwatch:GetMetricStatistics","cloudwatch:ListMetrics","cloudwatch:PutMetricAlarm","cloudwatch:GetMetricData","datapipeline:ActivatePipeline","datapipeline:CreatePipeline","datapipeline:DeletePipeline","datapipeline:DescribeObjects","datapipeline:DescribePipelines","datapipeline:GetPipelineDefinition","datapipeline:ListPipelines","datapipeline:PutPipelineDefinition","datapipeline:QueryObjects","ec2:DescribeVpcs","ec2:DescribeSubnets","ec2:DescribeSecurityGroups","iam:GetRole","iam:ListRoles","kms:DescribeKey","kms:ListAliases","sns:CreateTopic","sns:DeleteTopic","sns:ListSubscriptions","sns:ListSubscriptionsByTopic","sns:ListTopics","sns:Subscribe","sns:Unsubscribe","sns:SetTopicAttributes","lambda:CreateFunction","lambda:ListFunctions","lambda:ListEventSourceMappings","lambda:CreateEventSourceMapping","lambda:DeleteEventSourceMapping","lambda:GetFunctionConfiguration","lambda:DeleteFunction","resource-groups:ListGroups","resource-groups:ListGroupResources","resource-groups:GetGroup","resource-groups:GetGroupQuery","resource-groups:DeleteGroup","resource-groups:CreateGroup","tag:GetResources","kinesis:ListStreams","kinesis:DescribeStream","kinesis:DescribeStreamSummary"],"Effect":"Allow","Resource":"*"},{"Action":"cloudwatch:GetInsightRuleReport","Effect":"Allow","Resource":"arn:aws:cloudwatch:*:*:insight-rule/DynamoDBContributorInsights*"},{"Action":["iam:PassRole"],"Effect":"Allow","Resource":"*","Condition":{"StringLike":{"iam:PassedToService":["application-autoscaling.amazonaws.com","application-autoscaling.amazonaws.com.cn","dax.amazonaws.com"]}}},{"Effect":"Allow","Action":["iam:CreateServiceLinkedRole"],"Resource":"*","Condition":{"StringEquals":{"iam:AWSServiceName":["replication.dynamodb.amazonaws.com","dax.amazonaws.com","dynamodb.application-autoscaling.amazonaws.com","contributorinsights.dynamodb.amazonaws.com","kinesisreplication.dynamodb.amazonaws.com"]}}}]}',
         "Provides full access to Amazon DynamoDB via the AWS Management Console."),
        ('AWSLambdaBasicExecutionRole',
         '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["logs:CreateLogGroup","logs:CreateLogStream","logs:PutLogEvents"],"Resource":"*"}]}',
         "Provides write permissions to CloudWatch Logs."),
        ('AWSLambdaVPCAccessExecutionRole',
         '{"Version":"2012-10-17","Statement":[{"Sid":"AWSLambdaVPCAccessExecutionPermissions","Effect":"Allow","Action":["logs:CreateLogGroup","logs:CreateLogStream","logs:PutLogEvents","ec2:CreateNetworkInterface","ec2:DescribeNetworkInterfaces","ec2:DescribeSubnets","ec2:DeleteNetworkInterface","ec2:AssignPrivateIpAddresses","ec2:UnassignPrivateIpAddresses"],"Resource":"*"}]}',
         "Provides minimum permissions for a Lambda function to execute while accessing a resource within a VPC - create, describe, delete network interfaces and write permissions to CloudWatch Logs."),
        ('AmazonSQSFullAccess',
         '{"Version":"2012-10-17","Statement":[{"Action":["sqs:*"],"Effect":"Allow","Resource":"*"}]}',
         "Provides full access to Amazon SQS via the AWS Management Console."),
        ('AmazonSNSFullAccess',
         '{"Version":"2012-10-17","Statement":[{"Sid":"SNSFullAccess","Effect":"Allow","Action":"sns:*","Resource":"*"},{"Sid":"SMSAccessViaSNS","Effect":"Allow","Action":["sms-voice:DescribeVerifiedDestinationNumbers","sms-voice:CreateVerifiedDestinationNumber","sms-voice:SendDestinationNumberVerificationCode","sms-voice:SendTextMessage","sms-voice:DeleteVerifiedDestinationNumber","sms-voice:VerifyDestinationNumber","sms-voice:DescribeAccountAttributes","sms-voice:DescribeSpendLimits","sms-voice:DescribePhoneNumbers","sms-voice:SetTextMessageSpendLimitOverride","sms-voice:DescribeOptedOutNumbers","sms-voice:DeleteOptedOutNumber"],"Resource":"*","Condition":{"StringEquals":{"aws:CalledViaLast":"sns.amazonaws.com"}}}]}',
         "Provides full access to Amazon SNS via the AWS Management Console."),
        ('AmazonECSTaskExecutionRolePolicy',
         '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["ecr:GetAuthorizationToken","ecr:BatchCheckLayerAvailability","ecr:GetDownloadUrlForLayer","ecr:BatchGetImage","logs:CreateLogStream","logs:PutLogEvents"],"Resource":"*"}]}',
         "Provides access to other AWS service resources that are required to run Amazon ECS tasks"),
        ('CloudWatchAgentServerPolicy',
         '{"Version":"2012-10-17","Statement":[{"Sid":"CWACloudWatchServerPermissions","Effect":"Allow","Action":["cloudwatch:PutMetricData","ec2:DescribeVolumes","ec2:DescribeTags","logs:PutLogEvents","logs:PutRetentionPolicy","logs:DescribeLogStreams","logs:DescribeLogGroups","logs:CreateLogStream","logs:CreateLogGroup","xray:PutTraceSegments","xray:PutTelemetryRecords","xray:GetSamplingRules","xray:GetSamplingTargets","xray:GetSamplingStatisticSummaries"],"Resource":"*"},{"Sid":"CWASSMServerPermissions","Effect":"Allow","Action":["ssm:GetParameter"],"Resource":"arn:aws:ssm:*:*:parameter/AmazonCloudWatch-*"}]}',
         "Permissions required to use AmazonCloudWatchAgent on servers"),
        ('CloudWatchLogsFullAccess',
         '{"Version":"2012-10-17","Statement":[{"Sid":"CloudWatchLogsFullAccess","Effect":"Allow","Action":["logs:*","cloudwatch:GenerateQuery","cloudwatch:GenerateQueryResultsSummary","observabilityadmin:GetS3TableIntegration","observabilityadmin:ListS3TableIntegrations","observabilityadmin:ListTelemetryPipelines"],"Resource":"*"}]}',
         "Provides full access to CloudWatch Logs"),
        ('AWSCloudFormationFullAccess',
         '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["cloudformation:*"],"Resource":"*"}]}',
         "Provides full access to AWS CloudFormation."),
        ('AmazonEKSClusterPolicy',
         '{"Version":"2012-10-17","Statement":[{"Sid":"AmazonEKSClusterPolicy","Effect":"Allow","Action":["autoscaling:DescribeAutoScalingGroups","autoscaling:UpdateAutoScalingGroup","ec2:AttachVolume","ec2:AuthorizeSecurityGroupIngress","ec2:CreateRoute","ec2:CreateSecurityGroup","ec2:CreateTags","ec2:CreateVolume","ec2:DeleteRoute","ec2:DeleteSecurityGroup","ec2:DeleteVolume","ec2:DescribeInstances","ec2:DescribeRouteTables","ec2:DescribeSecurityGroups","ec2:DescribeSubnets","ec2:DescribeVolumes","ec2:DescribeVolumesModifications","ec2:DescribeVpcs","ec2:DescribeDhcpOptions","ec2:DescribeNetworkInterfaces","ec2:DescribeAvailabilityZones","ec2:DetachVolume","ec2:ModifyInstanceAttribute","ec2:ModifyVolume","ec2:RevokeSecurityGroupIngress","ec2:DescribeAccountAttributes","ec2:DescribeAddresses","ec2:DescribeInternetGateways","ec2:DescribeInstanceTopology","elasticloadbalancing:AddTags","elasticloadbalancing:ApplySecurityGroupsToLoadBalancer","elasticloadbalancing:AttachLoadBalancerToSubnets","elasticloadbalancing:ConfigureHealthCheck","elasticloadbalancing:CreateListener","elasticloadbalancing:CreateLoadBalancer","elasticloadbalancing:CreateLoadBalancerListeners","elasticloadbalancing:CreateLoadBalancerPolicy","elasticloadbalancing:CreateTargetGroup","elasticloadbalancing:DeleteListener","elasticloadbalancing:DeleteLoadBalancer","elasticloadbalancing:DeleteLoadBalancerListeners","elasticloadbalancing:DeleteTargetGroup","elasticloadbalancing:DeregisterInstancesFromLoadBalancer","elasticloadbalancing:DeregisterTargets","elasticloadbalancing:DescribeListeners","elasticloadbalancing:DescribeLoadBalancerAttributes","elasticloadbalancing:DescribeLoadBalancerPolicies","elasticloadbalancing:DescribeLoadBalancers","elasticloadbalancing:DescribeTargetGroupAttributes","elasticloadbalancing:DescribeTargetGroups","elasticloadbalancing:DescribeTargetHealth","elasticloadbalancing:DetachLoadBalancerFromSubnets","elasticloadbalancing:ModifyListener","elasticloadbalancing:ModifyLoadBalancerAttributes","elasticloadbalancing:ModifyTargetGroup","elasticloadbalancing:ModifyTargetGroupAttributes","elasticloadbalancing:RegisterInstancesWithLoadBalancer","elasticloadbalancing:RegisterTargets","elasticloadbalancing:SetLoadBalancerPoliciesForBackendServer","elasticloadbalancing:SetLoadBalancerPoliciesOfListener","kms:DescribeKey"],"Resource":"*"},{"Sid":"AmazonEKSClusterPolicySLRCreate","Effect":"Allow","Action":"iam:CreateServiceLinkedRole","Resource":"*","Condition":{"StringEquals":{"iam:AWSServiceName":"elasticloadbalancing.amazonaws.com"}}},{"Sid":"AmazonEKSClusterPolicyENIDelete","Effect":"Allow","Action":"ec2:DeleteNetworkInterface","Resource":"*","Condition":{"StringEquals":{"ec2:ResourceTag/eks:eni:owner":"amazon-vpc-cni"}}}]}',
         "Provides Kubernetes the permissions it requires to manage resources on your behalf."),
        ('AmazonEKSWorkerNodePolicy',
         '{"Version":"2012-10-17","Statement":[{"Sid":"WorkerNodePermissions","Effect":"Allow","Action":["ec2:DescribeInstances","ec2:DescribeInstanceTypes","ec2:DescribeRouteTables","ec2:DescribeSecurityGroups","ec2:DescribeSubnets","ec2:DescribeVolumes","ec2:DescribeVolumesModifications","ec2:DescribeVpcs","eks:DescribeCluster","eks-auth:AssumeRoleForPodIdentity"],"Resource":"*"}]}',
         "This policy allows Amazon EKS worker nodes to connect to Amazon EKS Clusters."),
        ('AmazonEKS_CNI_Policy',
         '{"Version":"2012-10-17","Statement":[{"Sid":"AmazonEKSCNIPolicy","Effect":"Allow","Action":["ec2:AssignPrivateIpAddresses","ec2:AttachNetworkInterface","ec2:CreateNetworkInterface","ec2:DeleteNetworkInterface","ec2:DescribeInstances","ec2:DescribeTags","ec2:DescribeNetworkInterfaces","ec2:DescribeInstanceTypes","ec2:DescribeSubnets","ec2:DescribeSecurityGroups","ec2:DetachNetworkInterface","ec2:ModifyNetworkInterfaceAttribute","ec2:UnassignPrivateIpAddresses"],"Resource":"*"},{"Sid":"AmazonEKSCNIPolicyENITag","Effect":"Allow","Action":["ec2:CreateTags"],"Resource":["arn:aws:ec2:*:*:network-interface/*"]}]}',
         "Provides the Amazon VPC CNI Plugin (amazon-vpc-cni-k8s) the permissions it requires to modify the IP address configuration on your EKS worker nodes."),
        ('AmazonEKSServicePolicy',
         '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["ec2:CreateNetworkInterface","ec2:CreateNetworkInterfacePermission","ec2:DeleteNetworkInterface","ec2:DescribeInstances","ec2:DescribeNetworkInterfaces","ec2:DetachNetworkInterface","ec2:DescribeSecurityGroups","ec2:DescribeSubnets","ec2:DescribeVpcs","ec2:ModifyNetworkInterfaceAttribute","iam:ListAttachedRolePolicies","eks:UpdateClusterVersion","ec2:GetSecurityGroupsForVpc"],"Resource":"*"},{"Effect":"Allow","Action":["ec2:CreateTags","ec2:DeleteTags"],"Resource":["arn:aws:ec2:*:*:vpc/*","arn:aws:ec2:*:*:subnet/*"]},{"Effect":"Allow","Action":["ec2:CreateTags"],"Resource":["arn:aws:ec2:*:*:network-interface/*"],"Condition":{"StringLike":{"aws:RequestTag/Name":"eks-cluster-*"}}},{"Effect":"Allow","Action":"route53:AssociateVPCWithHostedZone","Resource":"*"},{"Effect":"Allow","Action":"logs:CreateLogGroup","Resource":"*"},{"Effect":"Allow","Action":["logs:CreateLogStream","logs:DescribeLogStreams"],"Resource":"arn:aws:logs:*:*:log-group:/aws/eks/*:*"},{"Effect":"Allow","Action":"logs:PutLogEvents","Resource":"arn:aws:logs:*:*:log-group:/aws/eks/*:*:*"},{"Effect":"Allow","Action":"iam:CreateServiceLinkedRole","Resource":"arn:aws:iam::*:role/aws-service-role/eks.amazonaws.com/AWSServiceRoleForAmazonEKS","Condition":{"StringLike":{"iam:AWSServiceName":"eks.amazonaws.com"}}}]}',
         "This policy allows Amazon Elastic Container Service for Kubernetes to create and manage the necessary resources to operate EKS Clusters."),
    ]

    for name, document, description in seeds:
        _aws_managed_policies[f"{_AWS_MANAGED_POLICY_PREFIX}{name}"] = _make_aws_managed_record(
            name, document, description,
        )


# ── Persistence ────────────────────────────────────────────

def get_state():
    return {
        "users": copy.deepcopy(_users),
        "roles": copy.deepcopy(_roles),
        "policies": copy.deepcopy(_policies),
        "groups": copy.deepcopy(_groups),
        "instance_profiles": copy.deepcopy(_instance_profiles),
        "access_keys": copy.deepcopy(_access_keys),
        "oidc_providers": copy.deepcopy(_oidc_providers),
        "service_linked_role_deletion_tasks": copy.deepcopy(_service_linked_role_deletion_tasks),
        "user_inline_policies": copy.deepcopy(_user_inline_policies),
        "aws_managed_attachment_counts": copy.deepcopy(_aws_managed_attachment_counts),
        "sla_jobs": copy.deepcopy(_sla_jobs),
        "saml_providers": copy.deepcopy(_saml_providers),
        "mfa_devices": copy.deepcopy(_mfa_devices),
        "login_profiles": copy.deepcopy(_login_profiles),
        "credential_reports": copy.deepcopy(_credential_reports),
        "account_password_policy": copy.deepcopy(_account_password_policy),
        "account_aliases": copy.deepcopy(_account_aliases),
    }


def restore_state(data):
    if data:
        _users.update(data.get("users", {}))
        _roles.update(data.get("roles", {}))
        _policies.update(data.get("policies", {}))
        _groups.update(data.get("groups", {}))
        _instance_profiles.update(data.get("instance_profiles", {}))
        _access_keys.update(data.get("access_keys", {}))
        _oidc_providers.update(data.get("oidc_providers", {}))
        _service_linked_role_deletion_tasks.update(data.get("service_linked_role_deletion_tasks", {}))
        _user_inline_policies.update(data.get("user_inline_policies", {}))
        _aws_managed_attachment_counts.update(data.get("aws_managed_attachment_counts", {}))
        _sla_jobs.update(data.get("sla_jobs", {}))
        _saml_providers.update(data.get("saml_providers", {}))
        _mfa_devices.update(data.get("mfa_devices", {}))
        _login_profiles.update(data.get("login_profiles", {}))
        _credential_reports.update(data.get("credential_reports", {}))
        _account_password_policy.update(data.get("account_password_policy", {}))
        _account_aliases.update(data.get("account_aliases", {}))


try:
    _restored = load_state("iam")
    if _restored:
        restore_state(_restored)
except Exception:
    import logging
    logging.getLogger(__name__).exception(
        "Failed to restore persisted state; continuing with fresh store"
    )


# ===================================================================== IAM
# =====================================================================

async def handle_request(method, path, headers, body, query_params):
    params = dict(query_params)
    content_type = headers.get("content-type", "")
    target = headers.get("x-amz-target", "")

    # JSON protocol (newer SDKs): X-Amz-Target: IAMService.ActionName
    if "amz-json" in content_type and "." in target:
        action_name = target.split(".")[-1]
        params["Action"] = [action_name]
        if body:
            try:
                json_body = json.loads(body)
                for k, v in json_body.items():
                    params[k] = [str(v)] if not isinstance(v, list) else v
            except (json.JSONDecodeError, TypeError):
                pass
    elif method == "POST" and body:
        for k, v in parse_qs(body.decode("utf-8", errors="replace")).items():
            params[k] = v

    action = _p(params, "Action")
    handler = _IAM_HANDLERS.get(action)
    if not handler:
        return _error(400, "InvalidAction", f"Unknown IAM action: {action}", ns="iam")
    return handler(params)


# -------------------- User management --------------------

def _create_user(p):
    name = _p(p, "UserName")
    if name in _users:
        return _error(409, "EntityAlreadyExists",
                      f"User with name {name} already exists.", ns="iam")
    path = _p(p, "Path") or "/"
    _users[name] = {
        "UserName": name,
        "Arn": f"arn:aws:iam::{get_account_id()}:user{path}{name}" if path != "/" else f"arn:aws:iam::{get_account_id()}:user/{name}",
        "UserId": _gen_id("AIDA"),
        "CreateDate": _now(),
        "Path": path,
        "AttachedPolicies": [],
        "Tags": _extract_tags(p),
    }
    return _xml(200, "CreateUserResponse",
                f"<CreateUserResult><User>{_user_xml(name)}</User></CreateUserResult>",
                ns="iam")


def _get_user(p):
    name = _p(p, "UserName")
    if not name:
        return _xml(200, "GetUserResponse",
                    "<GetUserResult><User>"
                    f"<UserName>root</UserName>"
                    f"<UserId>{get_account_id()}</UserId>"
                    f"<Arn>arn:aws:iam::{get_account_id()}:root</Arn>"
                    "<Path>/</Path>"
                    f"<CreateDate>{_now()}</CreateDate>"
                    "</User></GetUserResult>",
                    ns="iam")
    if name not in _users:
        return _error(404, "NoSuchEntity",
                      f"The user with name {name} cannot be found.", ns="iam")
    return _xml(200, "GetUserResponse",
                f"<GetUserResult><User>{_user_xml(name)}</User></GetUserResult>",
                ns="iam")


def _list_users(p):
    prefix = _p(p, "PathPrefix") or "/"
    members = "".join(
        f"<member>{_user_xml(n)}</member>"
        for n, u in _users.items()
        if u.get("Path", "/").startswith(prefix)
    )
    return _xml(200, "ListUsersResponse",
                f"<ListUsersResult><Users>{members}</Users>"
                "<IsTruncated>false</IsTruncated></ListUsersResult>",
                ns="iam")


def _delete_user(p):
    name = _p(p, "UserName")
    user = _users.get(name)
    if not user:
        return _error(404, "NoSuchEntity",
                      f"The user with name {name} cannot be found.", ns="iam")
    if user.get("AttachedPolicies"):
        return _error(409, "DeleteConflict",
                      "Cannot delete entity, must detach all policies first.", ns="iam")
    user_keys = [k for k, v in _access_keys.items() if v["UserName"] == name]
    if user_keys:
        return _error(409, "DeleteConflict",
                      "Cannot delete entity, must delete access keys first.", ns="iam")
    _users.pop(name, None)
    return _xml(200, "DeleteUserResponse", "", ns="iam")


# -------------------- Role management --------------------

def _create_role(p):
    name = _p(p, "RoleName")
    if name in _roles:
        return _error(409, "EntityAlreadyExists",
                      f"Role with name {name} already exists.", ns="iam")
    path = _p(p, "Path") or "/"
    _roles[name] = {
        "RoleName": name,
        "Arn": f"arn:aws:iam::{get_account_id()}:role{path}{name}" if path != "/" else f"arn:aws:iam::{get_account_id()}:role/{name}",
        "RoleId": _gen_id("AROA"),
        "CreateDate": _now(),
        "Path": path,
        "AssumeRolePolicyDocument": _p(p, "AssumeRolePolicyDocument"),
        "Description": _p(p, "Description"),
        "MaxSessionDuration": int(_p(p, "MaxSessionDuration") or 3600),
        "AttachedPolicies": [],
        "InlinePolicies": {},
        "Tags": _extract_tags(p),
    }
    return _xml(200, "CreateRoleResponse",
                f"<CreateRoleResult><Role>{_role_xml(name)}</Role></CreateRoleResult>",
                ns="iam")


def _get_role(p):
    name = _p(p, "RoleName")
    if name not in _roles:
        return _error(404, "NoSuchEntity",
                      f"Role {name} not found.", ns="iam")
    return _xml(200, "GetRoleResponse",
                f"<GetRoleResult><Role>{_role_xml(name)}</Role></GetRoleResult>",
                ns="iam")


def _list_roles(p):
    prefix = _p(p, "PathPrefix") or "/"
    members = "".join(
        f"<member>{_role_xml(n)}</member>"
        for n, r in _roles.items()
        if r.get("Path", "/").startswith(prefix)
    )
    return _xml(200, "ListRolesResponse",
                f"<ListRolesResult><Roles>{members}</Roles>"
                "<IsTruncated>false</IsTruncated></ListRolesResult>",
                ns="iam")


def _delete_role(p):
    name = _p(p, "RoleName")
    role = _roles.get(name)
    if not role:
        return _error(404, "NoSuchEntity",
                      f"Role {name} not found.", ns="iam")
    if role.get("AttachedPolicies"):
        return _error(409, "DeleteConflict",
                      "Cannot delete entity, must detach all policies first.", ns="iam")
    if role.get("InlinePolicies"):
        return _error(409, "DeleteConflict",
                      "Cannot delete entity, must delete all inline policies first.", ns="iam")
    for ip in _instance_profiles.values():
        if name in ip.get("Roles", []):
            return _error(409, "DeleteConflict",
                          "Cannot delete entity, must remove role from all instance profiles first.", ns="iam")
    _roles.pop(name, None)
    return _xml(200, "DeleteRoleResponse", "", ns="iam")


def _update_role(p):
    name = _p(p, "RoleName")
    role = _roles.get(name)
    if not role:
        return _error(404, "NoSuchEntity", f"Role {name} not found.", ns="iam")
    if "Description" in p:
        role["Description"] = _p(p, "Description")
    if "MaxSessionDuration" in p:
        role["MaxSessionDuration"] = int(_p(p, "MaxSessionDuration", "3600"))
    return _xml(200, "UpdateRoleResponse", "<UpdateRoleResult></UpdateRoleResult>", ns="iam")


def _update_assume_role_policy(p):
    name = _p(p, "RoleName")
    if name not in _roles:
        return _error(404, "NoSuchEntity",
                      f"Role {name} not found.", ns="iam")
    _roles[name]["AssumeRolePolicyDocument"] = _p(p, "PolicyDocument")
    return _xml(200, "UpdateAssumeRolePolicyResponse", "", ns="iam")


# -------------------- Managed policy management --------------------

def _create_policy(p):
    name = _p(p, "PolicyName")
    path = _p(p, "Path") or "/"
    arn = f"arn:aws:iam::{get_account_id()}:policy{path}{name}" if path != "/" else f"arn:aws:iam::{get_account_id()}:policy/{name}"
    if _is_aws_managed_arn(arn):
        return _error(400, "InvalidInput",
                      "Cannot create customer-managed policies under the reserved AWS-managed account.",
                      ns="iam")
    if arn in _policies:
        return _error(409, "EntityAlreadyExists",
                      f"A policy called {name} already exists.", ns="iam")
    doc = _p(p, "PolicyDocument")
    description = _p(p, "Description", "")
    # #445 (follow-up): CreatePolicy accepts Tags — honour them on create.
    create_tags = _extract_tags(p)
    policy_id = _gen_id("ANPA")
    version_id = "v1"
    _policies[arn] = {
        "PolicyName": name,
        "Arn": arn,
        "PolicyId": policy_id,
        "CreateDate": _now(),
        "UpdateDate": _now(),
        "DefaultVersionId": version_id,
        "AttachmentCount": 0,
        "IsAttachable": True,
        "Path": path,
        "Description": description,
        "Tags": list(create_tags or []),
        "Versions": {
            version_id: {
                "Document": doc,
                "VersionId": version_id,
                "IsDefaultVersion": True,
                "CreateDate": _now(),
            }
        },
    }
    return _xml(200, "CreatePolicyResponse",
                f"<CreatePolicyResult><Policy>{_managed_policy_xml(arn)}</Policy></CreatePolicyResult>",
                ns="iam")


def _get_policy(p):
    arn = _p(p, "PolicyArn")
    if not _policy_exists(arn):
        return _error(404, "NoSuchEntity",
                      f"Policy {arn} not found.", ns="iam")
    return _xml(200, "GetPolicyResponse",
                f"<GetPolicyResult><Policy>{_managed_policy_xml(arn)}</Policy></GetPolicyResult>",
                ns="iam")


def _get_policy_version(p):
    arn = _p(p, "PolicyArn")
    vid = _p(p, "VersionId")
    pol = _lookup_policy(arn)
    if not pol:
        return _error(404, "NoSuchEntity", "Policy not found.", ns="iam")
    ver = pol["Versions"].get(vid)
    if not ver:
        return _error(404, "NoSuchEntity",
                      f"Policy version {vid} not found.", ns="iam")
    doc = _url_quote(ver.get("Document") or "{}", safe="")
    is_default = "true" if ver.get("IsDefaultVersion") else "false"
    return _xml(200, "GetPolicyVersionResponse",
                f"<GetPolicyVersionResult><PolicyVersion>"
                f"<Document>{doc}</Document>"
                f"<VersionId>{vid}</VersionId>"
                f"<IsDefaultVersion>{is_default}</IsDefaultVersion>"
                f"<CreateDate>{ver['CreateDate']}</CreateDate>"
                f"</PolicyVersion></GetPolicyVersionResult>",
                ns="iam")


def _list_policy_versions(p):
    arn = _p(p, "PolicyArn")
    pol = _lookup_policy(arn)
    if not pol:
        return _error(404, "NoSuchEntity", "Policy not found.", ns="iam")
    members = ""
    for vid, ver in pol["Versions"].items():
        is_default = "true" if ver.get("IsDefaultVersion") else "false"
        members += (f"<member><VersionId>{vid}</VersionId>"
                    f"<IsDefaultVersion>{is_default}</IsDefaultVersion>"
                    f"<CreateDate>{ver['CreateDate']}</CreateDate></member>")
    return _xml(200, "ListPolicyVersionsResponse",
                f"<ListPolicyVersionsResult><Versions>{members}</Versions>"
                "<IsTruncated>false</IsTruncated></ListPolicyVersionsResult>",
                ns="iam")


def _create_policy_version(p):
    arn = _p(p, "PolicyArn")
    if _is_aws_managed_arn(arn):
        return _error(403, "AccessDenied",
                      f"Cannot create a new version on AWS-managed policy {arn}.",
                      ns="iam")
    pol = _policies.get(arn)
    if not pol:
        return _error(404, "NoSuchEntity", "Policy not found.", ns="iam")
    if len(pol["Versions"]) >= 5:
        return _error(409, "LimitExceeded",
                      "A managed policy can have at most 5 versions.", ns="iam")
    doc = _p(p, "PolicyDocument")
    set_default = _p(p, "SetAsDefault").lower() in ("true", "1") if _p(p, "SetAsDefault") else False
    next_v = max((int(v.lstrip("v")) for v in pol["Versions"]), default=0) + 1
    vid = f"v{next_v}"
    pol["Versions"][vid] = {
        "Document": doc,
        "VersionId": vid,
        "IsDefaultVersion": set_default,
        "CreateDate": _now(),
    }
    if set_default:
        for v in pol["Versions"].values():
            v["IsDefaultVersion"] = (v["VersionId"] == vid)
        pol["DefaultVersionId"] = vid
    pol["UpdateDate"] = _now()
    is_default = "true" if set_default else "false"
    return _xml(200, "CreatePolicyVersionResponse",
                f"<CreatePolicyVersionResult><PolicyVersion>"
                f"<VersionId>{vid}</VersionId>"
                f"<IsDefaultVersion>{is_default}</IsDefaultVersion>"
                f"<CreateDate>{pol['Versions'][vid]['CreateDate']}</CreateDate>"
                f"</PolicyVersion></CreatePolicyVersionResult>",
                ns="iam")


def _delete_policy_version(p):
    arn = _p(p, "PolicyArn")
    vid = _p(p, "VersionId")
    if _is_aws_managed_arn(arn):
        return _error(403, "AccessDenied",
                      f"Cannot delete versions of AWS-managed policy {arn}.",
                      ns="iam")
    pol = _policies.get(arn)
    if not pol:
        return _error(404, "NoSuchEntity", "Policy not found.", ns="iam")
    ver = pol["Versions"].get(vid)
    if not ver:
        return _error(404, "NoSuchEntity",
                      f"Policy version {vid} not found.", ns="iam")
    if ver.get("IsDefaultVersion"):
        return _error(409, "DeleteConflict",
                      "Cannot delete the default version of a policy.", ns="iam")
    del pol["Versions"][vid]
    return _xml(200, "DeletePolicyVersionResponse", "", ns="iam")


def _list_policies(p):
    scope = _p(p, "Scope") or "All"
    prefix = _p(p, "PathPrefix") or "/"
    members = ""
    # Customer-managed policies — always returned unless scope == "AWS".
    if scope != "AWS":
        for arn, pol in _policies.items():
            if not pol.get("Path", "/").startswith(prefix):
                continue
            members += f"<member>{_managed_policy_xml(arn)}</member>"
    # AWS-managed policies — returned for scope "All" or "AWS".
    if scope != "Local":
        for arn, pol in _aws_managed_policies.items():
            if not pol.get("Path", "/").startswith(prefix):
                continue
            members += f"<member>{_managed_policy_xml(arn)}</member>"
    return _xml(200, "ListPoliciesResponse",
                f"<ListPoliciesResult><Policies>{members}</Policies>"
                "<IsTruncated>false</IsTruncated></ListPoliciesResult>",
                ns="iam")


def _delete_policy(p):
    arn = _p(p, "PolicyArn")
    if _is_aws_managed_arn(arn):
        return _error(403, "AccessDenied",
                      f"Cannot delete AWS-managed policy {arn}.", ns="iam")
    if arn not in _policies:
        return _error(404, "NoSuchEntity", f"Policy {arn} not found.", ns="iam")
    pol = _policies[arn]
    if pol.get("AttachmentCount", 0) > 0:
        return _error(409, "DeleteConflict",
                      "Cannot delete a policy attached to entities.", ns="iam")
    del _policies[arn]
    return _xml(200, "DeletePolicyResponse", "", ns="iam")


# -------------------- List entities for policy --------------------

def _list_entities_for_policy(p):
    arn = _p(p, "PolicyArn")
    if not _policy_exists(arn):
        return _error(404, "NoSuchEntity", f"Policy {arn} not found.", ns="iam")
    entity_filter = _p(p, "EntityFilter") or ""
    path_prefix = _p(p, "PathPrefix") or "/"

    groups_xml = ""
    if entity_filter in ("", "Group"):
        for g in _groups.values():
            if not g.get("Path", "/").startswith(path_prefix):
                continue
            if arn in g.get("AttachedPolicies", []):
                groups_xml += (f"<member><GroupName>{g['GroupName']}</GroupName>"
                               f"<GroupId>{g['GroupId']}</GroupId></member>")

    roles_xml = ""
    if entity_filter in ("", "Role"):
        for r in _roles.values():
            if not r.get("Path", "/").startswith(path_prefix):
                continue
            if arn in r.get("AttachedPolicies", []):
                roles_xml += (f"<member><RoleName>{r['RoleName']}</RoleName>"
                              f"<RoleId>{r['RoleId']}</RoleId></member>")

    users_xml = ""
    if entity_filter in ("", "User"):
        for u in _users.values():
            if not u.get("Path", "/").startswith(path_prefix):
                continue
            if arn in u.get("AttachedPolicies", []):
                users_xml += (f"<member><UserName>{u['UserName']}</UserName>"
                              f"<UserId>{u['UserId']}</UserId></member>")

    return _xml(200, "ListEntitiesForPolicyResponse",
                f"<ListEntitiesForPolicyResult>"
                f"<PolicyGroups>{groups_xml}</PolicyGroups>"
                f"<PolicyRoles>{roles_xml}</PolicyRoles>"
                f"<PolicyUsers>{users_xml}</PolicyUsers>"
                f"<IsTruncated>false</IsTruncated>"
                f"</ListEntitiesForPolicyResult>",
                ns="iam")


# -------------------- Attached role policies --------------------

def _attach_role_policy(p):
    role_name = _p(p, "RoleName")
    policy_arn = _p(p, "PolicyArn")
    role = _roles.get(role_name)
    if not role:
        return _error(404, "NoSuchEntity",
                      f"Role {role_name} not found.", ns="iam")
    if policy_arn not in role["AttachedPolicies"]:
        role["AttachedPolicies"].append(policy_arn)
        if _is_aws_managed_arn(policy_arn):
            _bump_aws_managed_attachment(policy_arn, +1)
        else:
            pol = _policies.get(policy_arn)
            if pol:
                pol["AttachmentCount"] = pol.get("AttachmentCount", 0) + 1
    return _xml(200, "AttachRolePolicyResponse", "", ns="iam")


def _detach_role_policy(p):
    role_name = _p(p, "RoleName")
    policy_arn = _p(p, "PolicyArn")
    role = _roles.get(role_name)
    if not role:
        return _error(404, "NoSuchEntity",
                      f"Role {role_name} not found.", ns="iam")
    if policy_arn not in role["AttachedPolicies"]:
        return _error(404, "NoSuchEntity",
                      f"Policy {policy_arn} is not attached to role {role_name}.", ns="iam")
    role["AttachedPolicies"].remove(policy_arn)
    if _is_aws_managed_arn(policy_arn):
        _bump_aws_managed_attachment(policy_arn, -1)
    else:
        pol = _policies.get(policy_arn)
        if pol:
            pol["AttachmentCount"] = max(pol.get("AttachmentCount", 1) - 1, 0)
    return _xml(200, "DetachRolePolicyResponse", "", ns="iam")


def _list_attached_role_policies(p):
    role_name = _p(p, "RoleName")
    role = _roles.get(role_name)
    if not role:
        return _error(404, "NoSuchEntity",
                      f"Role {role_name} not found.", ns="iam")
    members = ""
    for arn in role["AttachedPolicies"]:
        pol = _lookup_policy(arn)
        pname = pol["PolicyName"] if pol else arn.rsplit("/", 1)[-1]
        members += (f"<member><PolicyName>{pname}</PolicyName>"
                    f"<PolicyArn>{arn}</PolicyArn></member>")
    return _xml(200, "ListAttachedRolePoliciesResponse",
                f"<ListAttachedRolePoliciesResult><AttachedPolicies>{members}</AttachedPolicies>"
                "<IsTruncated>false</IsTruncated></ListAttachedRolePoliciesResult>",
                ns="iam")


# -------------------- Inline role policies --------------------

def _put_role_policy(p):
    role_name = _p(p, "RoleName")
    policy_name = _p(p, "PolicyName")
    policy_doc = _p(p, "PolicyDocument")
    role = _roles.get(role_name)
    if not role:
        return _error(404, "NoSuchEntity",
                      f"Role {role_name} not found.", ns="iam")
    role["InlinePolicies"][policy_name] = policy_doc
    return _xml(200, "PutRolePolicyResponse", "", ns="iam")


def _get_role_policy(p):
    role_name = _p(p, "RoleName")
    policy_name = _p(p, "PolicyName")
    role = _roles.get(role_name)
    if not role:
        return _error(404, "NoSuchEntity",
                      f"Role {role_name} not found.", ns="iam")
    doc = role["InlinePolicies"].get(policy_name)
    if doc is None:
        return _error(404, "NoSuchEntity",
                      f"The role policy with name {policy_name} cannot be found.", ns="iam")
    if isinstance(doc, (dict, list)):
        doc_str = json.dumps(doc)
    elif isinstance(doc, (bytes, bytearray)):
        doc_str = doc.decode("utf-8")
    else:
        doc_str = doc
    encoded_doc = _url_quote(doc_str, safe="")
    return _xml(200, "GetRolePolicyResponse",
                f"<GetRolePolicyResult>"
                f"<RoleName>{role_name}</RoleName>"
                f"<PolicyName>{policy_name}</PolicyName>"
                f"<PolicyDocument>{encoded_doc}</PolicyDocument>"
                f"</GetRolePolicyResult>",
                ns="iam")


def _delete_role_policy(p):
    role_name = _p(p, "RoleName")
    policy_name = _p(p, "PolicyName")
    role = _roles.get(role_name)
    if not role:
        return _error(404, "NoSuchEntity",
                      f"Role {role_name} not found.", ns="iam")
    if policy_name not in role["InlinePolicies"]:
        return _error(404, "NoSuchEntity",
                      f"The role policy with name {policy_name} cannot be found.", ns="iam")
    del role["InlinePolicies"][policy_name]
    return _xml(200, "DeleteRolePolicyResponse", "", ns="iam")


def _list_role_policies(p):
    role_name = _p(p, "RoleName")
    role = _roles.get(role_name)
    if not role:
        return _error(404, "NoSuchEntity",
                      f"Role {role_name} not found.", ns="iam")
    members = "".join(
        f"<member>{name}</member>"
        for name in role["InlinePolicies"]
    )
    return _xml(200, "ListRolePoliciesResponse",
                f"<ListRolePoliciesResult><PolicyNames>{members}</PolicyNames>"
                "<IsTruncated>false</IsTruncated></ListRolePoliciesResult>",
                ns="iam")


# -------------------- Attached user policies --------------------

def _attach_user_policy(p):
    user_name = _p(p, "UserName")
    policy_arn = _p(p, "PolicyArn")
    user = _users.get(user_name)
    if not user:
        return _error(404, "NoSuchEntity",
                      f"The user with name {user_name} cannot be found.", ns="iam")
    if policy_arn not in user["AttachedPolicies"]:
        user["AttachedPolicies"].append(policy_arn)
        if _is_aws_managed_arn(policy_arn):
            _bump_aws_managed_attachment(policy_arn, +1)
        else:
            pol = _policies.get(policy_arn)
            if pol:
                pol["AttachmentCount"] = pol.get("AttachmentCount", 0) + 1
    return _xml(200, "AttachUserPolicyResponse", "", ns="iam")


def _detach_user_policy(p):
    user_name = _p(p, "UserName")
    policy_arn = _p(p, "PolicyArn")
    user = _users.get(user_name)
    if not user:
        return _error(404, "NoSuchEntity",
                      f"The user with name {user_name} cannot be found.", ns="iam")
    if policy_arn not in user["AttachedPolicies"]:
        return _error(404, "NoSuchEntity",
                      f"Policy {policy_arn} is not attached to user {user_name}.", ns="iam")
    user["AttachedPolicies"].remove(policy_arn)
    if _is_aws_managed_arn(policy_arn):
        _bump_aws_managed_attachment(policy_arn, -1)
    else:
        pol = _policies.get(policy_arn)
        if pol:
            pol["AttachmentCount"] = max(pol.get("AttachmentCount", 1) - 1, 0)
    return _xml(200, "DetachUserPolicyResponse", "", ns="iam")


def _list_attached_user_policies(p):
    user_name = _p(p, "UserName")
    user = _users.get(user_name)
    if not user:
        return _error(404, "NoSuchEntity",
                      f"The user with name {user_name} cannot be found.", ns="iam")
    members = ""
    for arn in user["AttachedPolicies"]:
        pol = _lookup_policy(arn)
        pname = pol["PolicyName"] if pol else arn.rsplit("/", 1)[-1]
        members += (f"<member><PolicyName>{pname}</PolicyName>"
                    f"<PolicyArn>{arn}</PolicyArn></member>")
    return _xml(200, "ListAttachedUserPoliciesResponse",
                f"<ListAttachedUserPoliciesResult><AttachedPolicies>{members}</AttachedPolicies>"
                "<IsTruncated>false</IsTruncated></ListAttachedUserPoliciesResult>",
                ns="iam")


# -------------------- Access keys --------------------

def _create_access_key(p):
    user_name = _p(p, "UserName")
    if not user_name:
        user_name = "default"
    if user_name != "default" and user_name not in _users:
        return _error(404, "NoSuchEntity",
                      f"The user with name {user_name} cannot be found.", ns="iam")
    key_id = _gen_access_key_id()
    secret = new_uuid().replace("-", "") + new_uuid().replace("-", "")[:8]
    _access_keys[key_id] = {
        "UserName": user_name,
        "AccessKeyId": key_id,
        "SecretAccessKey": secret,
        "Status": "Active",
        "CreateDate": _now(),
    }
    return _xml(200, "CreateAccessKeyResponse",
                f"<CreateAccessKeyResult><AccessKey>"
                f"<UserName>{user_name}</UserName>"
                f"<AccessKeyId>{key_id}</AccessKeyId>"
                f"<SecretAccessKey>{secret}</SecretAccessKey>"
                f"<Status>Active</Status>"
                f"<CreateDate>{_access_keys[key_id]['CreateDate']}</CreateDate>"
                f"</AccessKey></CreateAccessKeyResult>",
                ns="iam")


def _list_access_keys(p):
    user_name = _p(p, "UserName") or "default"
    members = ""
    for kid, v in _access_keys.items():
        if v["UserName"] == user_name:
            members += (f"<member><AccessKeyId>{kid}</AccessKeyId>"
                        f"<Status>{v['Status']}</Status>"
                        f"<UserName>{user_name}</UserName>"
                        f"<CreateDate>{v['CreateDate']}</CreateDate>"
                        f"</member>")
    return _xml(200, "ListAccessKeysResponse",
                f"<ListAccessKeysResult><AccessKeyMetadata>{members}</AccessKeyMetadata>"
                "<IsTruncated>false</IsTruncated></ListAccessKeysResult>",
                ns="iam")


def _delete_access_key(p):
    key_id = _p(p, "AccessKeyId")
    if key_id not in _access_keys:
        return _error(404, "NoSuchEntity",
                      f"The Access Key with id {key_id} cannot be found.", ns="iam")
    del _access_keys[key_id]
    return _xml(200, "DeleteAccessKeyResponse", "", ns="iam")


def _update_access_key(p):
    key_id = _p(p, "AccessKeyId")
    status = _p(p, "Status")
    user_name = _p(p, "UserName")

    if not key_id:
        return _error(400, "InvalidInput", "AccessKeyId is required.", ns="iam")
    if status not in ("Active", "Inactive"):
        return _error(400, "InvalidInput",
                      f"Invalid status value: {status!r}. Must be Active or Inactive.",
                      ns="iam")
    if key_id not in _access_keys:
        return _error(404, "NoSuchEntity",
                      f"The Access Key with id {key_id} cannot be found.", ns="iam")
    if user_name and _access_keys[key_id]["UserName"] != user_name:
        return _error(404, "NoSuchEntity",
                      f"The Access Key with id {key_id} cannot be found.", ns="iam")
    _access_keys[key_id]["Status"] = status
    return _xml(200, "UpdateAccessKeyResponse", "", ns="iam")


def _get_access_key_last_used(p):
    key_id = _p(p, "AccessKeyId")
    if not key_id:
        return _error(400, "InvalidInput", "AccessKeyId is required.", ns="iam")
    if key_id not in _access_keys:
        return _error(404, "NoSuchEntity",
                      f"The Access Key with id {key_id} cannot be found.", ns="iam")
    user_name = _access_keys[key_id]["UserName"]
    # Ministack does not track per-key usage; return the "never used" shape
    # that real AWS returns for keys that have never made a signed request
    # (no LastUsedDate element, Region/ServiceName = "N/A").
    return _xml(200, "GetAccessKeyLastUsedResponse",
                f"<GetAccessKeyLastUsedResult>"
                f"<UserName>{user_name}</UserName>"
                f"<AccessKeyLastUsed>"
                f"<Region>N/A</Region>"
                f"<ServiceName>N/A</ServiceName>"
                f"</AccessKeyLastUsed>"
                f"</GetAccessKeyLastUsedResult>",
                ns="iam")


# -------------------- Instance profiles --------------------

def _create_instance_profile(p):
    name = _p(p, "InstanceProfileName")
    if name in _instance_profiles:
        return _error(409, "EntityAlreadyExists",
                      f"Instance profile {name} already exists.", ns="iam")
    path = _p(p, "Path") or "/"
    ip_id = _gen_id("AIPA")
    arn = (f"arn:aws:iam::{get_account_id()}:instance-profile{path}{name}"
           if path != "/" else
           f"arn:aws:iam::{get_account_id()}:instance-profile/{name}")
    _instance_profiles[name] = {
        "InstanceProfileName": name,
        "InstanceProfileId": ip_id,
        "Arn": arn,
        "Path": path,
        "CreateDate": _now(),
        "Roles": [],
    }
    return _xml(200, "CreateInstanceProfileResponse",
                f"<CreateInstanceProfileResult>"
                f"<InstanceProfile>{_instance_profile_xml(name)}</InstanceProfile>"
                f"</CreateInstanceProfileResult>",
                ns="iam")


def _delete_instance_profile(p):
    name = _p(p, "InstanceProfileName")
    if name not in _instance_profiles:
        return _error(404, "NoSuchEntity",
                      f"Instance profile {name} not found.", ns="iam")
    ip = _instance_profiles[name]
    if ip["Roles"]:
        return _error(409, "DeleteConflict",
                      "Cannot delete entity, must remove all roles first.", ns="iam")
    del _instance_profiles[name]
    return _xml(200, "DeleteInstanceProfileResponse", "", ns="iam")


def _get_instance_profile(p):
    name = _p(p, "InstanceProfileName")
    if name not in _instance_profiles:
        return _error(404, "NoSuchEntity",
                      f"Instance profile {name} not found.", ns="iam")
    return _xml(200, "GetInstanceProfileResponse",
                f"<GetInstanceProfileResult>"
                f"<InstanceProfile>{_instance_profile_xml(name)}</InstanceProfile>"
                f"</GetInstanceProfileResult>",
                ns="iam")


def _add_role_to_instance_profile(p):
    ip_name = _p(p, "InstanceProfileName")
    role_name = _p(p, "RoleName")
    ip = _instance_profiles.get(ip_name)
    if not ip:
        return _error(404, "NoSuchEntity",
                      f"Instance profile {ip_name} not found.", ns="iam")
    if role_name not in _roles:
        return _error(404, "NoSuchEntity",
                      f"Role {role_name} not found.", ns="iam")
    if role_name in ip["Roles"]:
        return _error(409, "LimitExceeded",
                      f"Role {role_name} is already associated with instance profile {ip_name}.", ns="iam")
    if len(ip["Roles"]) >= 1:
        return _error(409, "LimitExceeded",
                      "An instance profile can have only one role.", ns="iam")
    ip["Roles"].append(role_name)
    return _xml(200, "AddRoleToInstanceProfileResponse", "", ns="iam")


def _remove_role_from_instance_profile(p):
    ip_name = _p(p, "InstanceProfileName")
    role_name = _p(p, "RoleName")
    ip = _instance_profiles.get(ip_name)
    if not ip:
        return _error(404, "NoSuchEntity",
                      f"Instance profile {ip_name} not found.", ns="iam")
    if role_name not in ip["Roles"]:
        return _error(404, "NoSuchEntity",
                      f"Role {role_name} is not associated with instance profile {ip_name}.", ns="iam")
    ip["Roles"].remove(role_name)
    return _xml(200, "RemoveRoleFromInstanceProfileResponse", "", ns="iam")


def _list_instance_profiles(p):
    prefix = _p(p, "PathPrefix") or "/"
    members = "".join(
        f"<member>{_instance_profile_xml(name)}</member>"
        for name, ip in _instance_profiles.items()
        if ip["Path"].startswith(prefix)
    )
    return _xml(200, "ListInstanceProfilesResponse",
                f"<ListInstanceProfilesResult><InstanceProfiles>{members}</InstanceProfiles>"
                "<IsTruncated>false</IsTruncated></ListInstanceProfilesResult>",
                ns="iam")


def _list_instance_profiles_for_role(p):
    role_name = _p(p, "RoleName")
    if role_name not in _roles:
        return _error(404, "NoSuchEntity",
                      f"Role {role_name} not found.", ns="iam")
    members = "".join(
        f"<member>{_instance_profile_xml(name)}</member>"
        for name, ip in _instance_profiles.items()
        if role_name in ip["Roles"]
    )
    return _xml(200, "ListInstanceProfilesForRoleResponse",
                f"<ListInstanceProfilesForRoleResult><InstanceProfiles>{members}</InstanceProfiles>"
                "<IsTruncated>false</IsTruncated></ListInstanceProfilesForRoleResult>",
                ns="iam")


# -------------------- Tags: roles --------------------

def _tag_role(p):
    role_name = _p(p, "RoleName")
    role = _roles.get(role_name)
    if not role:
        return _error(404, "NoSuchEntity",
                      f"Role {role_name} not found.", ns="iam")
    new_tags = _extract_tags(p)
    existing = {t["Key"]: t for t in role["Tags"]}
    for t in new_tags:
        existing[t["Key"]] = t
    role["Tags"] = list(existing.values())
    return _xml(200, "TagRoleResponse", "", ns="iam")


def _untag_role(p):
    role_name = _p(p, "RoleName")
    role = _roles.get(role_name)
    if not role:
        return _error(404, "NoSuchEntity",
                      f"Role {role_name} not found.", ns="iam")
    keys_to_remove = _extract_tag_keys(p)
    role["Tags"] = [t for t in role["Tags"] if t["Key"] not in keys_to_remove]
    return _xml(200, "UntagRoleResponse", "", ns="iam")


def _list_role_tags(p):
    role_name = _p(p, "RoleName")
    role = _roles.get(role_name)
    if not role:
        return _error(404, "NoSuchEntity",
                      f"Role {role_name} not found.", ns="iam")
    members = "".join(
        f"<member><Key>{t['Key']}</Key><Value>{t['Value']}</Value></member>"
        for t in role["Tags"]
    )
    return _xml(200, "ListRoleTagsResponse",
                f"<ListRoleTagsResult><Tags>{members}</Tags>"
                "<IsTruncated>false</IsTruncated></ListRoleTagsResult>",
                ns="iam")


# -------------------- Tags: users --------------------

def _tag_user(p):
    user_name = _p(p, "UserName")
    user = _users.get(user_name)
    if not user:
        return _error(404, "NoSuchEntity",
                      f"The user with name {user_name} cannot be found.", ns="iam")
    new_tags = _extract_tags(p)
    existing = {t["Key"]: t for t in user["Tags"]}
    for t in new_tags:
        existing[t["Key"]] = t
    user["Tags"] = list(existing.values())
    return _xml(200, "TagUserResponse", "", ns="iam")


def _untag_user(p):
    user_name = _p(p, "UserName")
    user = _users.get(user_name)
    if not user:
        return _error(404, "NoSuchEntity",
                      f"The user with name {user_name} cannot be found.", ns="iam")
    keys_to_remove = _extract_tag_keys(p)
    user["Tags"] = [t for t in user["Tags"] if t["Key"] not in keys_to_remove]
    return _xml(200, "UntagUserResponse", "", ns="iam")


def _list_user_tags(p):
    user_name = _p(p, "UserName")
    user = _users.get(user_name)
    if not user:
        return _error(404, "NoSuchEntity",
                      f"The user with name {user_name} cannot be found.", ns="iam")
    members = "".join(
        f"<member><Key>{t['Key']}</Key><Value>{t['Value']}</Value></member>"
        for t in user["Tags"]
    )
    return _xml(200, "ListUserTagsResponse",
                f"<ListUserTagsResult><Tags>{members}</Tags>"
                "<IsTruncated>false</IsTruncated></ListUserTagsResult>",
                ns="iam")


# -------------------- Simulate (stubs) --------------------

def _simulate_principal_policy(p):
    results = _build_simulate_results(p)
    return _xml(200, "SimulatePrincipalPolicyResponse",
                f"<SimulatePrincipalPolicyResult>"
                f"<EvaluationResults>{results}</EvaluationResults>"
                "<IsTruncated>false</IsTruncated>"
                f"</SimulatePrincipalPolicyResult>",
                ns="iam")


def _simulate_custom_policy(p):
    results = _build_simulate_results(p)
    return _xml(200, "SimulateCustomPolicyResponse",
                f"<SimulateCustomPolicyResult>"
                f"<EvaluationResults>{results}</EvaluationResults>"
                "<IsTruncated>false</IsTruncated>"
                f"</SimulateCustomPolicyResult>",
                ns="iam")


def _build_simulate_results(p):
    actions = []
    idx = 1
    while True:
        a = _p(p, f"ActionNames.member.{idx}")
        if not a:
            break
        actions.append(a)
        idx += 1
    if not actions:
        actions = ["sts:AssumeRole"]
    resource_arn = _p(p, "ResourceArns.member.1") or "*"
    members = ""
    for action in actions:
        members += (f"<member>"
                    f"<EvalActionName>{action}</EvalActionName>"
                    f"<EvalResourceName>{resource_arn}</EvalResourceName>"
                    f"<EvalDecision>allowed</EvalDecision>"
                    f"<MatchedStatements></MatchedStatements>"
                    f"<MissingContextValues></MissingContextValues>"
                    f"</member>")
    return members


# -------------------- Group management --------------------

def _create_group(p):
    name = _p(p, "GroupName")
    if name in _groups:
        return _error(409, "EntityAlreadyExists",
                      f"Group with name {name} already exists.", ns="iam")
    path = _p(p, "Path") or "/"
    _groups[name] = {
        "GroupName": name,
        "GroupId": _gen_id("AGPA"),
        "Arn": f"arn:aws:iam::{get_account_id()}:group{path}{name}" if path != "/" else f"arn:aws:iam::{get_account_id()}:group/{name}",
        "Path": path,
        "CreateDate": _now(),
        "Users": [],
    }
    return _xml(200, "CreateGroupResponse",
                f"<CreateGroupResult><Group>{_group_xml(name)}</Group></CreateGroupResult>",
                ns="iam")


def _get_group(p):
    name = _p(p, "GroupName")
    if name not in _groups:
        return _error(404, "NoSuchEntity",
                      f"The group with name {name} cannot be found.", ns="iam")
    g = _groups[name]
    user_members = ""
    for uname in g["Users"]:
        if uname in _users:
            user_members += f"<member>{_user_xml(uname)}</member>"
    return _xml(200, "GetGroupResponse",
                f"<GetGroupResult>"
                f"<Group>{_group_xml(name)}</Group>"
                f"<Users>{user_members}</Users>"
                f"<IsTruncated>false</IsTruncated>"
                f"</GetGroupResult>",
                ns="iam")


def _delete_group(p):
    name = _p(p, "GroupName")
    if name not in _groups:
        return _error(404, "NoSuchEntity",
                      f"The group with name {name} cannot be found.", ns="iam")
    _groups.pop(name, None)
    return _xml(200, "DeleteGroupResponse", "", ns="iam")


def _list_groups(p):
    prefix = _p(p, "PathPrefix") or "/"
    members = "".join(
        f"<member>{_group_xml(n)}</member>"
        for n, g in _groups.items()
        if g.get("Path", "/").startswith(prefix)
    )
    return _xml(200, "ListGroupsResponse",
                f"<ListGroupsResult><Groups>{members}</Groups>"
                "<IsTruncated>false</IsTruncated></ListGroupsResult>",
                ns="iam")


def _add_user_to_group(p):
    group_name = _p(p, "GroupName")
    user_name = _p(p, "UserName")
    g = _groups.get(group_name)
    if not g:
        return _error(404, "NoSuchEntity",
                      f"The group with name {group_name} cannot be found.", ns="iam")
    if user_name not in _users:
        return _error(404, "NoSuchEntity",
                      f"The user with name {user_name} cannot be found.", ns="iam")
    if user_name not in g["Users"]:
        g["Users"].append(user_name)
    return _xml(200, "AddUserToGroupResponse", "", ns="iam")


def _remove_user_from_group(p):
    group_name = _p(p, "GroupName")
    user_name = _p(p, "UserName")
    g = _groups.get(group_name)
    if not g:
        return _error(404, "NoSuchEntity",
                      f"The group with name {group_name} cannot be found.", ns="iam")
    if user_name not in g["Users"]:
        return _error(404, "NoSuchEntity",
                      f"The user with name {user_name} is not in group {group_name}.", ns="iam")
    g["Users"].remove(user_name)
    return _xml(200, "RemoveUserFromGroupResponse", "", ns="iam")


def _list_groups_for_user(p):
    user_name = _p(p, "UserName")
    if user_name not in _users:
        return _error(404, "NoSuchEntity",
                      f"The user with name {user_name} cannot be found.", ns="iam")
    members = "".join(
        f"<member>{_group_xml(n)}</member>"
        for n, g in _groups.items()
        if user_name in g["Users"]
    )
    return _xml(200, "ListGroupsForUserResponse",
                f"<ListGroupsForUserResult><Groups>{members}</Groups>"
                "<IsTruncated>false</IsTruncated></ListGroupsForUserResult>",
                ns="iam")


# -------------------- Inline user policies --------------------

def _put_user_policy(p):
    user_name = _p(p, "UserName")
    policy_name = _p(p, "PolicyName")
    policy_doc = _p(p, "PolicyDocument")
    if user_name not in _users:
        return _error(404, "NoSuchEntity",
                      f"The user with name {user_name} cannot be found.", ns="iam")
    user_policies = _user_inline_policies.get(user_name)
    if user_policies is None:
        user_policies = {}
        _user_inline_policies[user_name] = user_policies
    user_policies[policy_name] = policy_doc
    return _xml(200, "PutUserPolicyResponse", "", ns="iam")


def _get_user_policy(p):
    user_name = _p(p, "UserName")
    policy_name = _p(p, "PolicyName")
    if user_name not in _users:
        return _error(404, "NoSuchEntity",
                      f"The user with name {user_name} cannot be found.", ns="iam")
    doc = (_user_inline_policies.get(user_name) or {}).get(policy_name)
    if doc is None:
        return _error(404, "NoSuchEntity",
                      f"The user policy with name {policy_name} cannot be found.", ns="iam")
    if isinstance(doc, (dict, list)):
        doc_str = json.dumps(doc)
    elif isinstance(doc, (bytes, bytearray)):
        doc_str = doc.decode("utf-8")
    else:
        doc_str = doc
    encoded_doc = _url_quote(doc_str, safe="")
    return _xml(200, "GetUserPolicyResponse",
                f"<GetUserPolicyResult>"
                f"<UserName>{user_name}</UserName>"
                f"<PolicyName>{policy_name}</PolicyName>"
                f"<PolicyDocument>{encoded_doc}</PolicyDocument>"
                f"</GetUserPolicyResult>",
                ns="iam")


def _delete_user_policy(p):
    user_name = _p(p, "UserName")
    policy_name = _p(p, "PolicyName")
    if user_name not in _users:
        return _error(404, "NoSuchEntity",
                      f"The user with name {user_name} cannot be found.", ns="iam")
    user_policies = _user_inline_policies.get(user_name) or {}
    if policy_name not in user_policies:
        return _error(404, "NoSuchEntity",
                      f"The user policy with name {policy_name} cannot be found.", ns="iam")
    del user_policies[policy_name]
    return _xml(200, "DeleteUserPolicyResponse", "", ns="iam")


def _list_user_policies(p):
    user_name = _p(p, "UserName")
    if user_name not in _users:
        return _error(404, "NoSuchEntity",
                      f"The user with name {user_name} cannot be found.", ns="iam")
    members = "".join(
        f"<member>{pname}</member>"
        for pname in (_user_inline_policies.get(user_name) or {})
    )
    return _xml(200, "ListUserPoliciesResponse",
                f"<ListUserPoliciesResult><PolicyNames>{members}</PolicyNames>"
                "<IsTruncated>false</IsTruncated></ListUserPoliciesResult>",
                ns="iam")


# -------------------- Service-linked roles --------------------

def _create_service_linked_role(p):
    service_name = _p(p, "AWSServiceName")
    suffix = service_name.split(".")[0] if "." in service_name else service_name
    role_name = f"AWSServiceRoleFor{suffix.capitalize()}"
    path = f"/aws-service-role/{service_name}/"

    if role_name in _roles:
        return _error(409, "EntityAlreadyExists",
                      f"Role with name {role_name} already exists.", ns="iam")

    trust_policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": service_name},
            "Action": "sts:AssumeRole"
        }]
    })

    _roles[role_name] = {
        "RoleName": role_name,
        "Arn": f"arn:aws:iam::{get_account_id()}:role{path}{role_name}",
        "RoleId": _gen_id("AROA"),
        "CreateDate": _now(),
        "Path": path,
        "AssumeRolePolicyDocument": trust_policy,
        "Description": f"Service-linked role for {service_name}",
        "MaxSessionDuration": 3600,
        "AttachedPolicies": [],
        "InlinePolicies": {},
        "Tags": [],
    }
    return _xml(200, "CreateServiceLinkedRoleResponse",
                f"<CreateServiceLinkedRoleResult><Role>{_role_xml(role_name)}</Role></CreateServiceLinkedRoleResult>",
                ns="iam")


def _delete_service_linked_role(p):
    role_name = _p(p, "RoleName")
    role = _roles.get(role_name)
    if not role:
        return _error(404, "NoSuchEntity",
                      f"Role {role_name} not found.", ns="iam")

    if not role.get("Path", "").startswith("/aws-service-role/"):
        return _error(400, "InvalidInput",
                      f"Role {role_name} is not a service-linked role.", ns="iam")

    task_id = new_uuid()
    _service_linked_role_deletion_tasks[task_id] = {
        "Status": "SUCCEEDED",
        "RoleName": role_name,
    }
    _roles.pop(role_name, None)
    return _xml(200, "DeleteServiceLinkedRoleResponse",
                f"<DeleteServiceLinkedRoleResult><DeletionTaskId>{task_id}</DeletionTaskId></DeleteServiceLinkedRoleResult>",
                ns="iam")


def _get_service_linked_role_deletion_status(p):
    task_id = _p(p, "DeletionTaskId")
    task = _service_linked_role_deletion_tasks.get(task_id)
    if not task:
        return _error(404, "NoSuchEntity",
                      f"Deletion task {task_id} not found.", ns="iam")

    reason = ""
    if task["Status"] == "FAILED":
        reason = f"<Reason>{task.get('Reason', '')}</Reason>"

    return _xml(200, "GetServiceLinkedRoleDeletionStatusResponse",
                f"<GetServiceLinkedRoleDeletionStatusResult>"
                f"<Status>{task['Status']}</Status>"
                f"{reason}"
                f"</GetServiceLinkedRoleDeletionStatusResult>",
                ns="iam")


# -------------------- GetAccountAuthorizationDetails --------------------


def _get_account_authorization_details(p):
    # Extract Filter.member.N list; empty = all
    filters = set()
    idx = 1
    while True:
        f = _p(p, f"Filter.member.{idx}")
        if not f:
            break
        filters.add(f)
        idx += 1
    include_all = not filters

    # ---- UserDetailList ----
    user_detail_xml = ""
    if include_all or "User" in filters:
        for name, user in _users.items():
            # Inline user policies
            upols = _user_inline_policies.get(name) or {}
            inline_xml = "".join(
                f"<member>"
                f"<PolicyName>{pn}</PolicyName>"
                f"<PolicyDocument>{_url_quote(pd, safe='')}</PolicyDocument>"
                f"</member>"
                for pn, pd in upols.items()
            )
            # Attached managed policies
            attached_xml = ""
            for arn in user.get("AttachedPolicies", []):
                pol = _lookup_policy(arn)
                if pol:
                    attached_xml += (f"<member>"
                                     f"<PolicyName>{pol['PolicyName']}</PolicyName>"
                                     f"<PolicyArn>{arn}</PolicyArn>"
                                     f"</member>")
            # Groups the user belongs to
            group_xml = "".join(
                f"<member>{g['GroupName']}</member>"
                for g in _groups.values()
                if name in g.get("Users", [])
            )
            # Tags
            tags_xml = "".join(
                f"<member><Key>{t['Key']}</Key><Value>{t['Value']}</Value></member>"
                for t in user.get("Tags", [])
            )
            user_detail_xml += (
                f"<member>"
                f"<UserName>{user['UserName']}</UserName>"
                f"<UserId>{user['UserId']}</UserId>"
                f"<Arn>{user['Arn']}</Arn>"
                f"<Path>{user['Path']}</Path>"
                f"<CreateDate>{user['CreateDate']}</CreateDate>"
                f"<UserPolicyList>{inline_xml}</UserPolicyList>"
                f"<GroupList>{group_xml}</GroupList>"
                f"<AttachedManagedPolicies>{attached_xml}</AttachedManagedPolicies>"
                f"<Tags>{tags_xml}</Tags>"
                f"</member>"
            )

    # ---- GroupDetailList ----
    group_detail_xml = ""
    if include_all or "Group" in filters:
        for name, g in _groups.items():
            group_detail_xml += (
                f"<member>"
                f"<GroupName>{g['GroupName']}</GroupName>"
                f"<GroupId>{g['GroupId']}</GroupId>"
                f"<Arn>{g['Arn']}</Arn>"
                f"<Path>{g['Path']}</Path>"
                f"<CreateDate>{g['CreateDate']}</CreateDate>"
                f"<GroupPolicyList></GroupPolicyList>"
                f"<AttachedManagedPolicies></AttachedManagedPolicies>"
                f"</member>"
            )

    # ---- RoleDetailList ----
    role_detail_xml = ""
    if include_all or "Role" in filters:
        for name, role in _roles.items():
            assume_doc = _url_quote(role.get("AssumeRolePolicyDocument") or "{}", safe="")
            # Inline role policies
            inline_xml = "".join(
                f"<member>"
                f"<PolicyName>{pn}</PolicyName>"
                f"<PolicyDocument>{_url_quote(pd, safe='')}</PolicyDocument>"
                f"</member>"
                for pn, pd in (role.get("InlinePolicies") or {}).items()
            )
            # Attached managed policies
            attached_xml = ""
            for arn in role.get("AttachedPolicies", []):
                pol = _lookup_policy(arn)
                if pol:
                    attached_xml += (f"<member>"
                                     f"<PolicyName>{pol['PolicyName']}</PolicyName>"
                                     f"<PolicyArn>{arn}</PolicyArn>"
                                     f"</member>")
            # Instance profiles
            ip_xml = "".join(
                f"<member>{_instance_profile_xml(ipn)}</member>"
                for ipn, ip in _instance_profiles.items()
                if name in ip.get("Roles", [])
            )
            # Tags
            tags_xml = "".join(
                f"<member><Key>{t['Key']}</Key><Value>{t['Value']}</Value></member>"
                for t in role.get("Tags", [])
            )
            role_detail_xml += (
                f"<member>"
                f"<RoleName>{role['RoleName']}</RoleName>"
                f"<RoleId>{role['RoleId']}</RoleId>"
                f"<Arn>{role['Arn']}</Arn>"
                f"<Path>{role['Path']}</Path>"
                f"<CreateDate>{role['CreateDate']}</CreateDate>"
                f"<AssumeRolePolicyDocument>{assume_doc}</AssumeRolePolicyDocument>"
                f"<RolePolicyList>{inline_xml}</RolePolicyList>"
                f"<AttachedManagedPolicies>{attached_xml}</AttachedManagedPolicies>"
                f"<InstanceProfileList>{ip_xml}</InstanceProfileList>"
                f"<Tags>{tags_xml}</Tags>"
                f"</member>"
            )

    # ---- Policies (customer-managed) ----
    policies_xml = ""
    if include_all or "LocalManagedPolicy" in filters:
        for arn, pol in _policies.items():
            default_vid = pol.get("DefaultVersionId", "v1")
            versions_xml = "".join(
                f"<member>"
                f"<Document>{_url_quote(v.get('Document') or '{}', safe='')}</Document>"
                f"<VersionId>{v['VersionId']}</VersionId>"
                f"<IsDefaultVersion>{'true' if v.get('IsDefaultVersion') else 'false'}</IsDefaultVersion>"
                f"</member>"
                for v in pol.get("Versions", {}).values()
            )
            policies_xml += (
                f"<member>"
                f"<PolicyName>{pol['PolicyName']}</PolicyName>"
                f"<PolicyId>{pol['PolicyId']}</PolicyId>"
                f"<Arn>{arn}</Arn>"
                f"<Path>{pol.get('Path', '/')}</Path>"
                f"<DefaultVersionId>{default_vid}</DefaultVersionId>"
                f"<PolicyVersionList>{versions_xml}</PolicyVersionList>"
                f"</member>"
            )

    return _xml(200, "GetAccountAuthorizationDetailsResponse",
                f"<GetAccountAuthorizationDetailsResult>"
                f"<UserDetailList>{user_detail_xml}</UserDetailList>"
                f"<GroupDetailList>{group_detail_xml}</GroupDetailList>"
                f"<RoleDetailList>{role_detail_xml}</RoleDetailList>"
                f"<Policies>{policies_xml}</Policies>"
                f"<IsTruncated>false</IsTruncated>"
                f"</GetAccountAuthorizationDetailsResult>",
                ns="iam")


# -------------------- Virtual MFA devices --------------------


def _virtual_mfa_xml(serial, include_user=True):
    dev = _mfa_devices[serial]
    seed_b64 = base64.b64encode(dev["Base32StringSeed"].encode()).decode()
    # QRCode: tiny placeholder bytes, base64-encoded (blob field)
    qr_b64 = base64.b64encode(b"QR").decode()
    inner = (f"<SerialNumber>{dev['SerialNumber']}</SerialNumber>"
             f"<Base32StringSeed>{seed_b64}</Base32StringSeed>"
             f"<QRCodePNG>{qr_b64}</QRCodePNG>")
    if include_user and dev.get("User"):
        user_name = dev["User"]
        if user_name in _users:
            inner += f"<User>{_user_xml(user_name)}</User>"
        inner += f"<EnableDate>{dev['EnableDate']}</EnableDate>"
    return inner


def _create_virtual_mfa_device(p):
    name = _p(p, "VirtualMFADeviceName")
    path = _p(p, "Path") or "/"
    acct = get_account_id()
    if path == "/":
        serial = f"arn:aws:iam::{acct}:mfa/{name}"
    else:
        serial = f"arn:aws:iam::{acct}:mfa{path}{name}"
    if serial in _mfa_devices:
        return _error(409, "EntityAlreadyExists",
                      f"MFA device {serial} already exists.", ns="iam")
    seed = new_uuid().replace("-", "")[:32].upper()
    _mfa_devices[serial] = {
        "SerialNumber": serial,
        "Path": path,
        "CreateDate": _now(),
        "User": None,
        "EnableDate": None,
        "Base32StringSeed": seed,
        "Tags": _extract_tags(p),
    }
    return _xml(200, "CreateVirtualMFADeviceResponse",
                f"<CreateVirtualMFADeviceResult>"
                f"<VirtualMFADevice>{_virtual_mfa_xml(serial, include_user=False)}</VirtualMFADevice>"
                f"</CreateVirtualMFADeviceResult>",
                ns="iam")


def _enable_mfa_device(p):
    user_name = _p(p, "UserName")
    serial = _p(p, "SerialNumber")
    if user_name not in _users:
        return _error(404, "NoSuchEntity",
                      f"The user with name {user_name} cannot be found.", ns="iam")
    if serial not in _mfa_devices:
        return _error(404, "NoSuchEntity",
                      f"MFA device {serial} not found.", ns="iam")
    dev = _mfa_devices[serial]
    if dev["User"] is not None:
        return _error(409, "EntityAlreadyExists",
                      f"MFA device {serial} is already assigned.", ns="iam")
    dev["User"] = user_name
    dev["EnableDate"] = _now()
    return _xml(200, "EnableMFADeviceResponse", "", ns="iam")


def _deactivate_mfa_device(p):
    user_name = _p(p, "UserName")
    serial = _p(p, "SerialNumber")
    dev = _mfa_devices.get(serial)
    if dev is None or dev.get("User") != user_name:
        return _error(404, "NoSuchEntity",
                      f"MFA device {serial} is not assigned to user {user_name}.", ns="iam")
    dev["User"] = None
    dev["EnableDate"] = None
    return _xml(200, "DeactivateMFADeviceResponse", "", ns="iam")


def _resync_mfa_device(p):
    user_name = _p(p, "UserName")
    serial = _p(p, "SerialNumber")
    dev = _mfa_devices.get(serial)
    if dev is None or dev.get("User") != user_name:
        return _error(404, "NoSuchEntity",
                      f"MFA device {serial} not found or not assigned to {user_name}.", ns="iam")
    return _xml(200, "ResyncMFADeviceResponse", "", ns="iam")


def _list_mfa_devices(p):
    user_name = _p(p, "UserName")
    members = ""
    for serial, dev in _mfa_devices.items():
        if dev.get("User") == user_name:
            members += (f"<member>"
                        f"<UserName>{dev['User']}</UserName>"
                        f"<SerialNumber>{dev['SerialNumber']}</SerialNumber>"
                        f"<EnableDate>{dev['EnableDate']}</EnableDate>"
                        f"</member>")
    return _xml(200, "ListMFADevicesResponse",
                f"<ListMFADevicesResult>"
                f"<MFADevices>{members}</MFADevices>"
                f"<IsTruncated>false</IsTruncated>"
                f"</ListMFADevicesResult>",
                ns="iam")


def _list_virtual_mfa_devices(p):
    status = _p(p, "AssignmentStatus") or "Assigned"
    members = ""
    for serial, dev in _mfa_devices.items():
        is_assigned = dev.get("User") is not None
        if status == "Assigned" and not is_assigned:
            continue
        if status == "Unassigned" and is_assigned:
            continue
        member_xml = f"<SerialNumber>{dev['SerialNumber']}</SerialNumber>"
        if is_assigned:
            user_name = dev["User"]
            if user_name in _users:
                member_xml += f"<User>{_user_xml(user_name)}</User>"
            member_xml += f"<EnableDate>{dev['EnableDate']}</EnableDate>"
        members += f"<member>{member_xml}</member>"
    return _xml(200, "ListVirtualMFADevicesResponse",
                f"<ListVirtualMFADevicesResult>"
                f"<VirtualMFADevices>{members}</VirtualMFADevices>"
                f"<IsTruncated>false</IsTruncated>"
                f"</ListVirtualMFADevicesResult>",
                ns="iam")


def _delete_virtual_mfa_device(p):
    serial = _p(p, "SerialNumber")
    dev = _mfa_devices.get(serial)
    if dev is None:
        return _error(404, "NoSuchEntity",
                      f"MFA device {serial} not found.", ns="iam")
    if dev.get("User") is not None:
        return _error(409, "DeleteConflict",
                      f"MFA device {serial} is still assigned to a user.", ns="iam")
    del _mfa_devices[serial]
    return _xml(200, "DeleteVirtualMFADeviceResponse", "", ns="iam")
# -------------------- Login profiles --------------------


def _login_profile_xml(name):
    lp = _login_profiles[name]
    pwr = "true" if lp.get("PasswordResetRequired") else "false"
    return (f"<UserName>{lp['UserName']}</UserName>"
            f"<CreateDate>{lp['CreateDate']}</CreateDate>"
            f"<PasswordResetRequired>{pwr}</PasswordResetRequired>")


def _create_login_profile(p):
    name = _p(p, "UserName")
    if name not in _users:
        return _error(404, "NoSuchEntity",
                      f"The user with name {name} cannot be found.", ns="iam")
    if name in _login_profiles:
        return _error(409, "EntityAlreadyExists",
                      f"Login profile for user {name} already exists.", ns="iam")
    _login_profiles[name] = {
        "UserName": name,
        "CreateDate": _now(),
        "PasswordResetRequired": _p(p, "PasswordResetRequired", "false").lower() == "true",
    }
    return _xml(200, "CreateLoginProfileResponse",
                f"<CreateLoginProfileResult>"
                f"<LoginProfile>{_login_profile_xml(name)}</LoginProfile>"
                f"</CreateLoginProfileResult>",
                ns="iam")


def _get_login_profile(p):
    name = _p(p, "UserName")
    if name not in _login_profiles:
        return _error(404, "NoSuchEntity",
                      f"Login profile for user {name} cannot be found.", ns="iam")
    return _xml(200, "GetLoginProfileResponse",
                f"<GetLoginProfileResult>"
                f"<LoginProfile>{_login_profile_xml(name)}</LoginProfile>"
                f"</GetLoginProfileResult>",
                ns="iam")


def _update_login_profile(p):
    name = _p(p, "UserName")
    if name not in _login_profiles:
        return _error(404, "NoSuchEntity",
                      f"Login profile for user {name} cannot be found.", ns="iam")
    pwr = _p(p, "PasswordResetRequired", "")
    if pwr:
        _login_profiles[name]["PasswordResetRequired"] = pwr.lower() == "true"
    return _xml(200, "UpdateLoginProfileResponse", "", ns="iam")


def _delete_login_profile(p):
    name = _p(p, "UserName")
    if name not in _login_profiles:
        return _error(404, "NoSuchEntity",
                      f"Login profile for user {name} cannot be found.", ns="iam")
    del _login_profiles[name]
    return _xml(200, "DeleteLoginProfileResponse", "", ns="iam")


# -------------------- Credential report --------------------

_CRED_REPORT_HEADER = (
    "user,arn,user_creation_time,password_enabled,password_last_used,"
    "password_last_changed,password_next_rotation,mfa_active,"
    "access_key_1_active,access_key_1_last_rotated,access_key_1_last_used_date,"
    "access_key_1_last_used_region,access_key_1_last_used_service,"
    "access_key_2_active,access_key_2_last_rotated,access_key_2_last_used_date,"
    "access_key_2_last_used_region,access_key_2_last_used_service,"
    "cert_1_active,cert_1_last_rotated,cert_2_active,cert_2_last_rotated"
)


def _build_credential_report():
    acct = get_account_id()
    rows = [_CRED_REPORT_HEADER]

    # Synthetic root account row
    rows.append(
        f"<root_account>,arn:aws:iam::{acct}:root,{_now()},"
        "not_supported,not_supported,not_supported,not_supported,false,"
        "false,N/A,N/A,N/A,N/A,"
        "false,N/A,N/A,N/A,N/A,"
        "false,N/A,false,N/A"
    )

    # One row per user
    for name, user in _users.items():
        has_password = name in _login_profiles
        has_mfa = any(
            dev.get("User") == name for dev in _mfa_devices.values()
        )

        # Collect up to 2 active access keys (sorted by creation for stable output)
        user_keys = sorted(
            [v for v in _access_keys.values() if v["UserName"] == name],
            key=lambda k: k.get("CreateDate", ""),
        )
        key1 = user_keys[0] if len(user_keys) > 0 else None
        key2 = user_keys[1] if len(user_keys) > 1 else None

        def _key_cols(k):
            if k is None:
                return "false,N/A,N/A,N/A,N/A"
            active = "true" if k.get("Status") == "Active" else "false"
            rotated = k.get("CreateDate", "N/A")
            return f"{active},{rotated},N/A,N/A,N/A"

        row = (
            f"{name},{user['Arn']},{user['CreateDate']},"
            f"{'true' if has_password else 'false'},"
            f"N/A,N/A,N/A,"
            f"{'true' if has_mfa else 'false'},"
            f"{_key_cols(key1)},"
            f"{_key_cols(key2)},"
            "false,N/A,false,N/A"
        )
        rows.append(row)

    return "\n".join(rows)


def _generate_credential_report(p):
    _credential_reports["report"] = {
        "content": _build_credential_report(),
        "generated_time": _now(),
    }
    return _xml(200, "GenerateCredentialReportResponse",
                "<GenerateCredentialReportResult>"
                "<State>COMPLETE</State>"
                "<Description>No report exists. Starting a new report generation task</Description>"
                "</GenerateCredentialReportResult>",
                ns="iam")


def _get_credential_report(p):
    report = _credential_reports.get("report")
    if not report:
        return _error(410, "ReportNotPresent",
                      "No credential report exists. Generate one with GenerateCredentialReport.", ns="iam")
    content_b64 = base64.b64encode(report["content"].encode("utf-8")).decode()
    return _xml(200, "GetCredentialReportResponse",
                f"<GetCredentialReportResult>"
                f"<Content>{content_b64}</Content>"
                f"<ReportFormat>text/csv</ReportFormat>"
                f"<GeneratedTime>{report['generated_time']}</GeneratedTime>"
                f"</GetCredentialReportResult>",
                ns="iam")


# -------------------- OIDC providers --------------------

def _create_oidc_provider(p):
    url = _p(p, "Url")
    client_ids = []
    idx = 1
    while True:
        cid = _p(p, f"ClientIDList.member.{idx}")
        if not cid:
            break
        client_ids.append(cid)
        idx += 1
    thumbprints = []
    idx = 1
    while True:
        tp = _p(p, f"ThumbprintList.member.{idx}")
        if not tp:
            break
        thumbprints.append(tp)
        idx += 1

    host = url.replace("https://", "").replace("http://", "").rstrip("/")
    arn = f"arn:aws:iam::{get_account_id()}:oidc-provider/{host}"

    if arn in _oidc_providers:
        return _error(409, "EntityAlreadyExists",
                      f"OIDC provider with url {url} already exists.", ns="iam")

    tags = _extract_tags(p)
    _oidc_providers[arn] = {
        "Url": url,
        "ClientIDList": client_ids,
        "ThumbprintList": thumbprints,
        "Arn": arn,
        "CreateDate": _now(),
        "Tags": tags,
    }
    return _xml(200, "CreateOpenIDConnectProviderResponse",
                f"<CreateOpenIDConnectProviderResult>"
                f"<OpenIDConnectProviderArn>{arn}</OpenIDConnectProviderArn>"
                f"</CreateOpenIDConnectProviderResult>",
                ns="iam")


def _get_oidc_provider(p):
    arn = _p(p, "OpenIDConnectProviderArn")
    prov = _oidc_providers.get(arn)
    if not prov:
        return _error(404, "NoSuchEntity",
                      f"OIDC provider {arn} not found.", ns="iam")
    client_members = "".join(f"<member>{c}</member>" for c in prov["ClientIDList"])
    thumb_members = "".join(f"<member>{t}</member>" for t in prov["ThumbprintList"])
    tag_members = "".join(
        f"<member><Key>{t['Key']}</Key><Value>{t['Value']}</Value></member>"
        for t in prov.get("Tags", [])
    )
    return _xml(200, "GetOpenIDConnectProviderResponse",
                f"<GetOpenIDConnectProviderResult>"
                f"<Url>{prov['Url']}</Url>"
                f"<ClientIDList>{client_members}</ClientIDList>"
                f"<ThumbprintList>{thumb_members}</ThumbprintList>"
                f"<CreateDate>{prov['CreateDate']}</CreateDate>"
                f"<Tags>{tag_members}</Tags>"
                f"</GetOpenIDConnectProviderResult>",
                ns="iam")


def _delete_oidc_provider(p):
    arn = _p(p, "OpenIDConnectProviderArn")
    if arn not in _oidc_providers:
        return _error(404, "NoSuchEntity",
                      f"OIDC provider {arn} not found.", ns="iam")
    del _oidc_providers[arn]
    return _xml(200, "DeleteOpenIDConnectProviderResponse", "", ns="iam")


# -------------------- Service last accessed (Access Advisor) --------------------


def _generate_service_last_accessed_details(p):
    arn = _p(p, "Arn")
    job_id = new_uuid()
    _sla_jobs[job_id] = {"Arn": arn, "JobStatus": "COMPLETED", "JobCreationDate": _now()}
    return _xml(200, "GenerateServiceLastAccessedDetailsResponse",
                f"<GenerateServiceLastAccessedDetailsResult>"
                f"<JobId>{job_id}</JobId>"
                f"</GenerateServiceLastAccessedDetailsResult>",
                ns="iam")


def _get_service_last_accessed_details(p):
    job_id = _p(p, "JobId")
    job = _sla_jobs.get(job_id)
    if not job:
        return _error(404, "NoSuchEntity",
                      f"Job {job_id} not found.", ns="iam")
    return _xml(200, "GetServiceLastAccessedDetailsResponse",
                f"<GetServiceLastAccessedDetailsResult>"
                f"<JobStatus>{job['JobStatus']}</JobStatus>"
                f"<JobCreationDate>{job['JobCreationDate']}</JobCreationDate>"
                f"<JobCompletionDate>{job['JobCreationDate']}</JobCompletionDate>"
                f"<ServicesLastAccessed></ServicesLastAccessed>"
                f"<IsTruncated>false</IsTruncated>"
                f"</GetServiceLastAccessedDetailsResult>",
                ns="iam")


def _list_oidc_providers(p):
    members = "".join(
        f"<member><Arn>{arn}</Arn></member>"
        for arn in _oidc_providers
    )
    return _xml(200, "ListOpenIDConnectProvidersResponse",
                f"<ListOpenIDConnectProvidersResult>"
                f"<OpenIDConnectProviderList>{members}</OpenIDConnectProviderList>"
                f"</ListOpenIDConnectProvidersResult>",
                ns="iam")


# -------------------- SAML providers --------------------


def _create_saml_provider(p):
    name = _p(p, "Name")
    acct = get_account_id()
    arn = f"arn:aws:iam::{acct}:saml-provider/{name}"
    if arn in _saml_providers:
        return _error(409, "EntityAlreadyExists",
                      f"SAML provider {arn} already exists.", ns="iam")
    metadata = _p(p, "SAMLMetadataDocument")
    _saml_providers[arn] = {
        "Arn": arn,
        "Name": name,
        "SAMLMetadataDocument": metadata,
        "CreateDate": _now(),
        "ValidUntil": _future(365 * 24 * 3600),
        "Tags": _extract_tags(p),
    }
    return _xml(200, "CreateSAMLProviderResponse",
                f"<CreateSAMLProviderResult>"
                f"<SAMLProviderArn>{arn}</SAMLProviderArn>"
                f"</CreateSAMLProviderResult>",
                ns="iam")


def _get_saml_provider(p):
    arn = _p(p, "SAMLProviderArn")
    prov = _saml_providers.get(arn)
    if not prov:
        return _error(404, "NoSuchEntity",
                      f"SAML provider {arn} not found.", ns="iam")
    tag_members = "".join(
        f"<member><Key>{t['Key']}</Key><Value>{t['Value']}</Value></member>"
        for t in prov.get("Tags", [])
    )
    return _xml(200, "GetSAMLProviderResponse",
                f"<GetSAMLProviderResult>"
                f"<SAMLMetadataDocument>{_xml_escape(prov['SAMLMetadataDocument'])}</SAMLMetadataDocument>"
                f"<CreateDate>{prov['CreateDate']}</CreateDate>"
                f"<ValidUntil>{prov['ValidUntil']}</ValidUntil>"
                f"<Tags>{tag_members}</Tags>"
                f"</GetSAMLProviderResult>",
                ns="iam")


def _list_saml_providers(p):
    members = "".join(
        f"<member>"
        f"<Arn>{prov['Arn']}</Arn>"
        f"<ValidUntil>{prov['ValidUntil']}</ValidUntil>"
        f"<CreateDate>{prov['CreateDate']}</CreateDate>"
        f"</member>"
        for prov in _saml_providers.values()
    )
    return _xml(200, "ListSAMLProvidersResponse",
                f"<ListSAMLProvidersResult>"
                f"<SAMLProviderList>{members}</SAMLProviderList>"
                f"</ListSAMLProvidersResult>",
                ns="iam")


def _update_saml_provider(p):
    arn = _p(p, "SAMLProviderArn")
    prov = _saml_providers.get(arn)
    if not prov:
        return _error(404, "NoSuchEntity",
                      f"SAML provider {arn} not found.", ns="iam")
    prov["SAMLMetadataDocument"] = _p(p, "SAMLMetadataDocument")
    return _xml(200, "UpdateSAMLProviderResponse",
                f"<UpdateSAMLProviderResult>"
                f"<SAMLProviderArn>{arn}</SAMLProviderArn>"
                f"</UpdateSAMLProviderResult>",
                ns="iam")


def _delete_saml_provider(p):
    arn = _p(p, "SAMLProviderArn")
    if arn not in _saml_providers:
        return _error(404, "NoSuchEntity",
                      f"SAML provider {arn} not found.", ns="iam")
    del _saml_providers[arn]
    return _xml(200, "DeleteSAMLProviderResponse", "", ns="iam")


# -------------------- Account summary / password policy / aliases --------------------


def _get_account_summary(p):
    num_users = len(_users)
    num_groups = len(_groups)
    num_roles = len(_roles)
    num_policies = len(_policies)
    num_mfa = len(_mfa_devices)
    num_mfa_in_use = sum(1 for d in _mfa_devices.values() if d.get("User") is not None)
    acct_mfa_enabled = 1 if num_mfa_in_use > 0 else 0

    summary = {
        "Users": num_users,
        "UsersQuota": 5000,
        "Groups": num_groups,
        "GroupsQuota": 300,
        "GroupsPerUserQuota": 10,
        "Roles": num_roles,
        "RolesQuota": 1000,
        "Policies": num_policies,
        "PoliciesQuota": 1500,
        "PolicySizeQuota": 6144,
        "PolicyVersionsInUse": num_policies,
        "PolicyVersionsInUseQuota": 10000,
        "MFADevices": num_mfa,
        "MFADevicesInUse": num_mfa_in_use,
        "AccountMFAEnabled": acct_mfa_enabled,
        "AccountAccessKeysPresent": 0,
        "AccountSigningCertificatesPresent": 0,
        "AccessKeysPerUserQuota": 2,
        "AssumeRolePolicySizeQuota": 2048,
        "GroupPolicySizeQuota": 5120,
        "UserPolicySizeQuota": 2048,
        "AttachedPoliciesPerGroupQuota": 10,
        "AttachedPoliciesPerRoleQuota": 10,
        "AttachedPoliciesPerUserQuota": 10,
        "VersionsPerPolicyQuota": 5,
        "ServerCertificates": 0,
        "ServerCertificatesQuota": 20,
        "SigningCertificatesPerUserQuota": 2,
    }
    entries = "".join(
        f"<entry><key>{k}</key><value>{v}</value></entry>"
        for k, v in summary.items()
    )
    return _xml(200, "GetAccountSummaryResponse",
                f"<GetAccountSummaryResult>"
                f"<SummaryMap>{entries}</SummaryMap>"
                f"</GetAccountSummaryResult>",
                ns="iam")


def _get_account_password_policy(p):
    pol = _account_password_policy.get("policy")
    if not pol:
        return _error(404, "NoSuchEntity",
                      "The account password policy does not exist.", ns="iam")
    fields = (
        f"<MinimumPasswordLength>{pol.get('MinimumPasswordLength', 8)}</MinimumPasswordLength>"
        f"<RequireSymbols>{'true' if pol.get('RequireSymbols') else 'false'}</RequireSymbols>"
        f"<RequireNumbers>{'true' if pol.get('RequireNumbers') else 'false'}</RequireNumbers>"
        f"<RequireUppercaseCharacters>{'true' if pol.get('RequireUppercaseCharacters') else 'false'}</RequireUppercaseCharacters>"
        f"<RequireLowercaseCharacters>{'true' if pol.get('RequireLowercaseCharacters') else 'false'}</RequireLowercaseCharacters>"
        f"<AllowUsersToChangePassword>{'true' if pol.get('AllowUsersToChangePassword') else 'false'}</AllowUsersToChangePassword>"
        f"<ExpirePasswords>{'true' if pol.get('MaxPasswordAge') else 'false'}</ExpirePasswords>"
        f"<HardExpiry>{'true' if pol.get('HardExpiry') else 'false'}</HardExpiry>"
    )
    if pol.get("MaxPasswordAge"):
        fields += f"<MaxPasswordAge>{pol['MaxPasswordAge']}</MaxPasswordAge>"
    if pol.get("PasswordReusePrevention"):
        fields += f"<PasswordReusePrevention>{pol['PasswordReusePrevention']}</PasswordReusePrevention>"
    return _xml(200, "GetAccountPasswordPolicyResponse",
                f"<GetAccountPasswordPolicyResult>"
                f"<PasswordPolicy>{fields}</PasswordPolicy>"
                f"</GetAccountPasswordPolicyResult>",
                ns="iam")


def _update_account_password_policy(p):
    pol = _account_password_policy.get("policy") or {}
    for field in ("MinimumPasswordLength", "MaxPasswordAge", "PasswordReusePrevention"):
        val = _p(p, field, "")
        if val:
            pol[field] = int(val)
    for field in ("RequireSymbols", "RequireNumbers", "RequireUppercaseCharacters",
                  "RequireLowercaseCharacters", "AllowUsersToChangePassword", "HardExpiry"):
        val = _p(p, field, "")
        if val:
            pol[field] = val.lower() == "true"
    _account_password_policy["policy"] = pol
    return _xml(200, "UpdateAccountPasswordPolicyResponse", "", ns="iam")


def _delete_account_password_policy(p):
    if "policy" not in _account_password_policy:
        return _error(404, "NoSuchEntity",
                      "The account password policy does not exist.", ns="iam")
    del _account_password_policy["policy"]
    return _xml(200, "DeleteAccountPasswordPolicyResponse", "", ns="iam")


def _list_account_aliases(p):
    aliases = _account_aliases.get("aliases") or []
    members = "".join(f"<member>{a}</member>" for a in aliases)
    return _xml(200, "ListAccountAliasesResponse",
                f"<ListAccountAliasesResult>"
                f"<AccountAliases>{members}</AccountAliases>"
                f"<IsTruncated>false</IsTruncated>"
                f"</ListAccountAliasesResult>",
                ns="iam")


def _create_account_alias(p):
    alias = _p(p, "AccountAlias")
    _account_aliases["aliases"] = [alias]
    return _xml(200, "CreateAccountAliasResponse", "", ns="iam")


def _delete_account_alias(p):
    alias = _p(p, "AccountAlias")
    aliases = _account_aliases.get("aliases") or []
    if alias in aliases:
        aliases.remove(alias)
        _account_aliases["aliases"] = aliases
    return _xml(200, "DeleteAccountAliasResponse", "", ns="iam")


# -------------------- Tags: policies --------------------

def _tag_policy(p):
    arn = _p(p, "PolicyArn")
    if _is_aws_managed_arn(arn):
        return _error(403, "AccessDenied",
                      f"Cannot tag AWS-managed policy {arn}.", ns="iam")
    pol = _policies.get(arn)
    if not pol:
        return _error(404, "NoSuchEntity",
                      f"Policy {arn} not found.", ns="iam")
    new_tags = _extract_tags(p)
    existing = {t["Key"]: t for t in pol.get("Tags", [])}
    for t in new_tags:
        existing[t["Key"]] = t
    pol["Tags"] = list(existing.values())
    return _xml(200, "TagPolicyResponse", "", ns="iam")


def _untag_policy(p):
    arn = _p(p, "PolicyArn")
    if _is_aws_managed_arn(arn):
        return _error(403, "AccessDenied",
                      f"Cannot untag AWS-managed policy {arn}.", ns="iam")
    pol = _policies.get(arn)
    if not pol:
        return _error(404, "NoSuchEntity",
                      f"Policy {arn} not found.", ns="iam")
    keys_to_remove = _extract_tag_keys(p)
    pol["Tags"] = [t for t in pol.get("Tags", []) if t["Key"] not in keys_to_remove]
    return _xml(200, "UntagPolicyResponse", "", ns="iam")


def _list_policy_tags(p):
    arn = _p(p, "PolicyArn")
    pol = _lookup_policy(arn)
    if not pol:
        return _error(404, "NoSuchEntity",
                      f"Policy {arn} not found.", ns="iam")
    members = "".join(
        f"<member><Key>{t['Key']}</Key><Value>{t['Value']}</Value></member>"
        for t in pol.get("Tags", [])
    )
    return _xml(200, "ListPolicyTagsResponse",
                f"<ListPolicyTagsResult><Tags>{members}</Tags>"
                "<IsTruncated>false</IsTruncated></ListPolicyTagsResult>",
                ns="iam")




# ===================================================================== Shared helpers
# =====================================================================

def _p(params, key, default=""):
    val = params.get(key, [default])
    return val[0] if isinstance(val, list) else val


def _now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _future(seconds):
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + seconds))


def _gen_id(prefix="AIDA"):
    return prefix + new_uuid().replace("-", "")[:17].upper()


def _gen_access_key_id():
    return "AKIA" + new_uuid().replace("-", "")[:16].upper()


def _gen_session_access_key():
    return "ASIA" + new_uuid().replace("-", "")[:16].upper()


def _gen_secret():
    raw = new_uuid().replace("-", "") + new_uuid().replace("-", "")
    return raw[:40]


def _gen_session_token():
    parts = [new_uuid().replace("-", "") for _ in range(4)]
    return "FwoGZX" + "".join(parts)


def _extract_tags(p):
    tags = []
    idx = 1
    while True:
        key = _p(p, f"Tags.member.{idx}.Key")
        if not key:
            break
        value = _p(p, f"Tags.member.{idx}.Value")
        tags.append({"Key": key, "Value": value})
        idx += 1
    return tags


def _extract_tag_keys(p):
    keys = set()
    idx = 1
    while True:
        key = _p(p, f"TagKeys.member.{idx}")
        if not key:
            break
        keys.add(key)
        idx += 1
    return keys


# -------------------- XML builders --------------------

def _user_xml(name):
    u = _users[name]
    # Tags are emitted when present (#441). _role_xml follows the same pattern.
    tags_xml = ""
    if u.get("Tags"):
        tag_members = "".join(
            f"<member><Key>{t['Key']}</Key><Value>{t['Value']}</Value></member>"
            for t in u["Tags"]
        )
        tags_xml = f"<Tags>{tag_members}</Tags>"
    return (f"<UserName>{u['UserName']}</UserName>"
            f"<UserId>{u['UserId']}</UserId>"
            f"<Arn>{u['Arn']}</Arn>"
            f"<Path>{u['Path']}</Path>"
            f"<CreateDate>{u['CreateDate']}</CreateDate>"
            f"{tags_xml}")


def _role_xml(name):
    r = _roles[name]
    assume_doc = _url_quote(r.get("AssumeRolePolicyDocument") or "{}", safe="")
    desc = r.get("Description") or ""
    max_dur = r.get("MaxSessionDuration", 3600)
    tags_xml = ""
    if r.get("Tags"):
        tag_members = "".join(
            f"<member><Key>{t['Key']}</Key><Value>{t['Value']}</Value></member>"
            for t in r["Tags"]
        )
        tags_xml = f"<Tags>{tag_members}</Tags>"
    return (f"<RoleName>{r['RoleName']}</RoleName>"
            f"<RoleId>{r['RoleId']}</RoleId>"
            f"<Arn>{r['Arn']}</Arn>"
            f"<Path>{r['Path']}</Path>"
            f"<CreateDate>{r['CreateDate']}</CreateDate>"
            f"<AssumeRolePolicyDocument>{assume_doc}</AssumeRolePolicyDocument>"
            f"<Description>{desc}</Description>"
            f"<MaxSessionDuration>{max_dur}</MaxSessionDuration>"
            f"{tags_xml}")


def _managed_policy_xml(arn):
    pol = _lookup_policy(arn)
    # Description is omitted when empty to match real AWS (#438).
    description = pol.get("Description") or ""
    description_xml = f"<Description>{description}</Description>" if description else ""
    # Tags emission mirrors _user_xml / _role_xml (#445). TagPolicy mutates
    # _policies[arn]["Tags"]; without this block GetPolicy/ListPolicies drop
    # them and Terraform re-adds default_tags on every apply.
    tags_xml = ""
    if pol.get("Tags"):
        tag_members = "".join(
            f"<member><Key>{t['Key']}</Key><Value>{t['Value']}</Value></member>"
            for t in pol["Tags"]
        )
        tags_xml = f"<Tags>{tag_members}</Tags>"
    # AWS-managed AttachmentCount is per-(session-account, arn); the
    # shared global record carries 0 and the real value lives in
    # ``_aws_managed_attachment_counts``. Customer-managed records
    # carry their own counter directly.
    if _is_aws_managed_arn(arn):
        attachment_count = _aws_managed_attachment_counts.get(arn, 0)
    else:
        attachment_count = pol.get("AttachmentCount", 0)
    return (f"<PolicyName>{pol['PolicyName']}</PolicyName>"
            f"<Arn>{arn}</Arn>"
            f"<PolicyId>{pol['PolicyId']}</PolicyId>"
            f"<DefaultVersionId>{pol['DefaultVersionId']}</DefaultVersionId>"
            f"<AttachmentCount>{attachment_count}</AttachmentCount>"
            f"<IsAttachable>true</IsAttachable>"
            f"<CreateDate>{pol['CreateDate']}</CreateDate>"
            f"<UpdateDate>{pol.get('UpdateDate', pol['CreateDate'])}</UpdateDate>"
            f"<Path>{pol.get('Path', '/')}</Path>"
            f"{description_xml}"
            f"{tags_xml}")


def _group_xml(name):
    g = _groups[name]
    return (f"<GroupName>{g['GroupName']}</GroupName>"
            f"<GroupId>{g['GroupId']}</GroupId>"
            f"<Arn>{g['Arn']}</Arn>"
            f"<Path>{g['Path']}</Path>"
            f"<CreateDate>{g['CreateDate']}</CreateDate>")


def _instance_profile_xml(name):
    ip = _instance_profiles[name]
    roles_xml = ""
    for rname in ip["Roles"]:
        if rname in _roles:
            roles_xml += f"<member>{_role_xml(rname)}</member>"
    return (f"<InstanceProfileName>{ip['InstanceProfileName']}</InstanceProfileName>"
            f"<InstanceProfileId>{ip['InstanceProfileId']}</InstanceProfileId>"
            f"<Arn>{ip['Arn']}</Arn>"
            f"<Path>{ip['Path']}</Path>"
            f"<CreateDate>{ip['CreateDate']}</CreateDate>"
            f"<Roles>{roles_xml}</Roles>")


def _xml(status, root_tag, inner, ns="iam"):
    ns_url = {
        "iam": "https://iam.amazonaws.com/doc/2010-05-08/",
        "sts": "https://sts.amazonaws.com/doc/2011-06-15/",
    }.get(ns, "")
    body = (f'<?xml version="1.0" encoding="UTF-8"?>'
            f'<{root_tag} xmlns="{ns_url}">'
            f'{inner}'
            f'<ResponseMetadata><RequestId>{new_uuid()}</RequestId></ResponseMetadata>'
            f'</{root_tag}>').encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


def _error(status, code, message, ns="iam"):
    ns_url = {
        "iam": "https://iam.amazonaws.com/doc/2010-05-08/",
        "sts": "https://sts.amazonaws.com/doc/2011-06-15/",
    }.get(ns, "")
    body = (f'<?xml version="1.0" encoding="UTF-8"?>'
            f'<ErrorResponse xmlns="{ns_url}">'
            f'<Error><Code>{code}</Code><Message>{message}</Message></Error>'
            f'<RequestId>{new_uuid()}</RequestId>'
            f'</ErrorResponse>').encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


# -------------------- Handler dispatch table --------------------

_IAM_HANDLERS = {
    "CreateUser": _create_user,
    "GetUser": _get_user,
    "ListUsers": _list_users,
    "DeleteUser": _delete_user,
    "CreateRole": _create_role,
    "GetRole": _get_role,
    "ListRoles": _list_roles,
    "DeleteRole": _delete_role,
    "UpdateRole": _update_role,
    "CreatePolicy": _create_policy,
    "GetPolicy": _get_policy,
    "GetPolicyVersion": _get_policy_version,
    "ListPolicyVersions": _list_policy_versions,
    "CreatePolicyVersion": _create_policy_version,
    "DeletePolicyVersion": _delete_policy_version,
    "ListPolicies": _list_policies,
    "DeletePolicy": _delete_policy,
    "ListEntitiesForPolicy": _list_entities_for_policy,
    "AttachRolePolicy": _attach_role_policy,
    "DetachRolePolicy": _detach_role_policy,
    "ListAttachedRolePolicies": _list_attached_role_policies,
    "PutRolePolicy": _put_role_policy,
    "GetRolePolicy": _get_role_policy,
    "DeleteRolePolicy": _delete_role_policy,
    "ListRolePolicies": _list_role_policies,
    "AttachUserPolicy": _attach_user_policy,
    "DetachUserPolicy": _detach_user_policy,
    "ListAttachedUserPolicies": _list_attached_user_policies,
    "CreateAccessKey": _create_access_key,
    "ListAccessKeys": _list_access_keys,
    "DeleteAccessKey": _delete_access_key,
    "UpdateAccessKey": _update_access_key,
    "GetAccessKeyLastUsed": _get_access_key_last_used,
    "CreateInstanceProfile": _create_instance_profile,
    "DeleteInstanceProfile": _delete_instance_profile,
    "GetInstanceProfile": _get_instance_profile,
    "AddRoleToInstanceProfile": _add_role_to_instance_profile,
    "RemoveRoleFromInstanceProfile": _remove_role_from_instance_profile,
    "ListInstanceProfiles": _list_instance_profiles,
    "ListInstanceProfilesForRole": _list_instance_profiles_for_role,
    "UpdateAssumeRolePolicy": _update_assume_role_policy,
    "TagRole": _tag_role,
    "UntagRole": _untag_role,
    "ListRoleTags": _list_role_tags,
    "TagUser": _tag_user,
    "UntagUser": _untag_user,
    "ListUserTags": _list_user_tags,
    "SimulatePrincipalPolicy": _simulate_principal_policy,
    "SimulateCustomPolicy": _simulate_custom_policy,
    "CreateGroup": _create_group,
    "GetGroup": _get_group,
    "DeleteGroup": _delete_group,
    "ListGroups": _list_groups,
    "AddUserToGroup": _add_user_to_group,
    "RemoveUserFromGroup": _remove_user_from_group,
    "ListGroupsForUser": _list_groups_for_user,
    "PutUserPolicy": _put_user_policy,
    "GetUserPolicy": _get_user_policy,
    "DeleteUserPolicy": _delete_user_policy,
    "ListUserPolicies": _list_user_policies,
    "CreateServiceLinkedRole": _create_service_linked_role,
    "DeleteServiceLinkedRole": _delete_service_linked_role,
    "GetServiceLinkedRoleDeletionStatus": _get_service_linked_role_deletion_status,
    "CreateOpenIDConnectProvider": _create_oidc_provider,
    "GetOpenIDConnectProvider": _get_oidc_provider,
    "DeleteOpenIDConnectProvider": _delete_oidc_provider,
    "GenerateServiceLastAccessedDetails": _generate_service_last_accessed_details,
    "GetServiceLastAccessedDetails": _get_service_last_accessed_details,
    "CreateSAMLProvider": _create_saml_provider,
    "GetSAMLProvider": _get_saml_provider,
    "ListSAMLProviders": _list_saml_providers,
    "UpdateSAMLProvider": _update_saml_provider,
    "DeleteSAMLProvider": _delete_saml_provider,
    "ListOpenIDConnectProviders": _list_oidc_providers,
    "GetAccountAuthorizationDetails": _get_account_authorization_details,
    "CreateVirtualMFADevice": _create_virtual_mfa_device,
    "EnableMFADevice": _enable_mfa_device,
    "DeactivateMFADevice": _deactivate_mfa_device,
    "ResyncMFADevice": _resync_mfa_device,
    "ListMFADevices": _list_mfa_devices,
    "ListVirtualMFADevices": _list_virtual_mfa_devices,
    "DeleteVirtualMFADevice": _delete_virtual_mfa_device,
    "CreateLoginProfile": _create_login_profile,
    "GetLoginProfile": _get_login_profile,
    "UpdateLoginProfile": _update_login_profile,
    "DeleteLoginProfile": _delete_login_profile,
    "GenerateCredentialReport": _generate_credential_report,
    "GetCredentialReport": _get_credential_report,
    "GetAccountSummary": _get_account_summary,
    "GetAccountPasswordPolicy": _get_account_password_policy,
    "UpdateAccountPasswordPolicy": _update_account_password_policy,
    "DeleteAccountPasswordPolicy": _delete_account_password_policy,
    "ListAccountAliases": _list_account_aliases,
    "CreateAccountAlias": _create_account_alias,
    "DeleteAccountAlias": _delete_account_alias,
    "TagPolicy": _tag_policy,
    "UntagPolicy": _untag_policy,
    "ListPolicyTags": _list_policy_tags,
}


def reset():
    _users.clear()
    _roles.clear()
    _policies.clear()
    _access_keys.clear()
    _instance_profiles.clear()
    _groups.clear()
    _user_inline_policies.clear()
    _oidc_providers.clear()
    _service_linked_role_deletion_tasks.clear()
    _sla_jobs.clear()
    _saml_providers.clear()
    _mfa_devices.clear()
    _login_profiles.clear()
    _credential_reports.clear()
    _account_password_policy.clear()
    _account_aliases.clear()
    # Re-seed AWS-managed policies. They're not customer state, so a
    # reset call should restore the canonical set rather than leave the
    # store empty. Per-account attachment counters are session state
    # though, so they get cleared.
    _aws_managed_policies.clear()
    _aws_managed_attachment_counts.clear()
    _seed_aws_managed_policies()


# Populate AWS-managed policies after all helper definitions are in
# scope (``_make_aws_managed_record`` references ``_now`` / ``_gen_id``
# defined further down).
_seed_aws_managed_policies()
