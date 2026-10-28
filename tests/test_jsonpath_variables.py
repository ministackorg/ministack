"""JSONPath workflow variables survive output projection and nested callbacks."""
import unittest
from unittest.mock import patch
from ministack.services import stepfunctions as sfn


class JSONPathVariablesTest(unittest.TestCase):
    def test_task_assignment_uses_raw_result_and_survives_projection(self):
        ctx = {"State": {}, "variables": {"target": None}}
        task = {"Type": "Task", "Resource": "arn:aws:states:::lambda:invoke",
                "Assign": {"target.$": "$.Payload.address"},
                "OutputPath": "$.Payload.value", "End": True}
        with patch.object(sfn, "_invoke_resource", return_value={"Payload": {"address": {"id": "physical"}, "value": {}}}), patch.object(sfn, "_add_event"):
            output, _ = sfn._execute_task(task, {}, {}, ctx)
        self.assertEqual(output, {})
        self.assertEqual(sfn._resolve_params_obj({"target.$": "$target"}, output, ctx), {"target": {"id": "physical"}})
        self.assertEqual(sfn._resolve_params_obj({"id.$": "States.Format('{}', $target.id)"}, {}, ctx), {"id": "physical"})

    def test_assignments_read_previous_values(self):
        ctx = {"variables": {"a": 1, "b": 2}}
        sfn._execute_pass({"Type": "Pass", "Assign": {"a.$": "$b", "b.$": "$a"}, "End": True}, {}, ctx)
        self.assertEqual(ctx["variables"], {"a": 2, "b": 1})

    def test_choice_assignment_and_default(self):
        for flag in (True, False):
            ctx = {"variables": {}}
            state = {"Type": "Choice", "Choices": [{"Variable": "$.flag", "BooleanEquals": True, "Assign": {"chosen": True}, "Next": "Done"}], "Assign": {"chosen": False}, "Default": "Done"}
            sfn._execute_choice(state, {"flag": flag}, ctx)
            self.assertEqual(ctx["variables"]["chosen"], flag)

    def test_nested_parallel_reads_outer_variable_without_leaking_assignment(self):
        ctx = {"State": {}, "variables": {"target": {"id": "physical"}}}
        branch = {"StartAt": "Read", "States": {"Read": {
            "Type": "Pass", "Parameters": {"address.$": "$target.id"},
            "Assign": {"branchOnly": "value"}, "End": True}}}
        output, _ = sfn._execute_parallel({"Type": "Parallel", "Branches": [branch, branch], "End": True}, {}, {}, ctx)
        self.assertEqual(output, [{"address": "physical"}, {"address": "physical"}])
        self.assertNotIn("branchOnly", ctx["variables"])

    def test_map_assignment_uses_aggregate_result(self):
        ctx = {"State": {}, "variables": {"target": "outer"}}
        state = {"Type": "Map", "ItemsPath": "$.items", "Iterator": {
            "StartAt": "Read", "States": {"Read": {"Type": "Pass",
            "Parameters": {"target.$": "$target"}, "End": True}}},
            "Assign": {"results.$": "$"}, "ResultPath": None, "End": True}
        output, _ = sfn._execute_map(state, {"items": [1, 2]}, {}, ctx)
        self.assertEqual(output, {"items": [1, 2]})
        self.assertEqual(ctx["variables"]["results"], [{"target": "outer"}] * 2)

    def test_wait_assignment_uses_filtered_input(self):
        ctx = {"variables": {}}
        sfn._execute_wait({"Type": "Wait", "Seconds": 0, "InputPath": "$.value",
                           "Assign": {"saved.$": "$"}, "End": True},
                          {"value": {"id": 3}}, ctx)
        self.assertEqual(ctx["variables"]["saved"], {"id": 3})

    def test_undefined_variable_fails(self):
        with self.assertRaises(sfn._ExecutionError):
            sfn._resolve_params_obj({"value.$": "$missing"}, {}, {"variables": {}})


if __name__ == "__main__":
    unittest.main()
