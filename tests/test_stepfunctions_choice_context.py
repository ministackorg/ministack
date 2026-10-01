import json
import time
import uuid

import pytest

from ministack.services import stepfunctions


@pytest.mark.parametrize(
    'rule',
    [
        {'Variable': '$$.Execution.Input.marker', 'IsPresent': True},
        {'Variable': '$$.Execution.Input.missing', 'IsPresent': False},
        {'Variable': '$$.Execution.Input.marker', 'StringEquals': 'keep'},
        {'And': [
            {'Variable': '$$.Execution.Input.marker', 'IsPresent': True},
            {'Or': [
                {'Variable': '$.marker', 'StringEquals': 'wrong'},
                {'Not': {'Variable': '$$.Execution.Input.marker', 'StringEquals': 'wrong'}},
            ]},
        ]},
        {'Variable': '$.marker', 'StringEqualsPath': '$$.Execution.Input.marker'},
        {'Variable': '$$.Execution.Input.marker', 'StringEqualsPath': '$.marker'},
        {'Variable': '$$.Execution.Input.marker', 'StringEqualsPath': '$$.Execution.Input.marker'},
        {'Variable': '$.number', 'NumericEqualsPath': '$$.Execution.Input.number'},
        {'Variable': '$.number', 'NumericLessThanPath': '$$.Execution.Input.higher'},
        {'Variable': '$.number', 'NumericGreaterThanPath': '$$.Execution.Input.lower'},
        {'Variable': '$.number', 'NumericLessThanEqualsPath': '$$.Execution.Input.number'},
        {'Variable': '$.number', 'NumericGreaterThanEqualsPath': '$$.Execution.Input.number'},
        {'Variable': '$.enabled', 'BooleanEqualsPath': '$$.Execution.Input.enabled'},
    ],
)
def test_choice_context_rules(rule):
    data = {'marker': 'keep', 'number': 5, 'enabled': True}
    ctx = {'Execution': {'Input': {**data, 'lower': 4, 'higher': 6}}}
    assert stepfunctions._evaluate_rule(rule, data, ctx)
    assert stepfunctions._execute_choice(
        {'InputPath': '$.projected', 'Choices': [{**rule, 'Next': 'Match'}], 'Default': 'Miss'},
        {'projected': data, 'marker': 'wrong', 'number': 0, 'enabled': False},
        ctx,
    ) == (data, 'Match')


def test_choice_context_missing_and_data_paths():
    assert not stepfunctions._evaluate_rule({'Variable': '$$.Execution.Id', 'IsPresent': True}, {})
    assert stepfunctions._evaluate_rule({'Variable': '$.value', 'NumericEqualsPath': '$.other'},
                                       {'value': 5, 'other': 5})
    assert not stepfunctions._evaluate_rule({'Variable': '$$.Execution.Input.marker', 'StringEquals': 'wrong'},
                                           {}, {'Execution': {'Input': {'marker': 'keep'}}})


def test_choice_context_survives_input_replacement(sfn):
    definition = {
        'StartAt': 'Replace',
        'States': {
            'Replace': {'Type': 'Pass', 'Result': {'projected': {'marker': 'keep'}}, 'Next': 'Check'},
            'Check': {
                'Type': 'Choice', 'InputPath': '$.projected',
                'Choices': [{'And': [
                    {'Variable': '$$.Execution.Input.marker', 'IsPresent': True},
                    {'Variable': '$.marker', 'StringEqualsPath': '$$.Execution.Input.marker'},
                    {'Variable': '$$.State.Name', 'StringEquals': 'Check'},
                ], 'Next': 'Match'}],
                'Default': 'Miss',
            },
            'Match': {'Type': 'Succeed'},
            'Miss': {'Type': 'Fail', 'Error': 'ContextNotResolved'},
        },
    }
    arn = sfn.create_state_machine(name=f'choice-context-{uuid.uuid4().hex}',
                                  definition=json.dumps(definition),
                                  roleArn='arn:aws:iam::000000000000:role/R')['stateMachineArn']
    try:
        execution = sfn.start_execution(stateMachineArn=arn, input=json.dumps({'marker': 'keep'}))['executionArn']
        for _ in range(100):
            result = sfn.describe_execution(executionArn=execution)
            if result['status'] != 'RUNNING':
                break
            time.sleep(0.1)
        assert result['status'] == 'SUCCEEDED', result
        assert json.loads(result['output']) == {'marker': 'keep'}
    finally:
        sfn.delete_state_machine(stateMachineArn=arn)
