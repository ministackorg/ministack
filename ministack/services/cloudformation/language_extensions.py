# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""The ``AWS::LanguageExtensions`` transform.

Applied to the template before the stack operation sees it: ``Fn::ForEach``
expands into the members it repeats, ``Fn::Length`` and ``Fn::ToJsonString``
become the number and the string they answer. The result is a plain template.

Where the identifier of a loop is substituted, as measured on an account: in
the keys of the fragment (``Param${Item}`` is ``Paramalpha``, ``&{Item}``
drops the non-alphanumeric characters), in the string of an ``Fn::Sub``
(``${Item}`` only; ``&{Item}`` stays literal) and through a ``Ref`` to the
identifier. A plain string value and a key nested below the fragment's own
keys keep their placeholders.

An account creates the stack record and rolls it back with the transform's
message; this refuses the call and leaves no stack, the shape of every other
pre-flight refusal in this engine. The sentences are the account's where they
were measured.

References: transform-aws-languageextensions, intrinsic-function-reference-
foreach, -length, -ToJsonString, -findinmap-enhancements.
"""

import json
import re

TRANSFORM = "AWS::LanguageExtensions"

# The macro-failure prefix, as AWS::Include answers it (measured).
_ERROR = f"Transform {TRANSFORM} failed with: "

_FOREACH = "Fn::ForEach::"
_LENGTH = "Fn::Length"
_TO_JSON = "Fn::ToJsonString"

# Sentences an account answers after the prefix (measured).
_LAYOUT_ERROR = "Fn::ForEach layout is incorrect"
_NO_COLLECTION = "Could not find a collection or could not be resolved for Fn::ForEach"
_LENGTH_UNRESOLVED = "The Fn::Length value could not be resolved for properties"
# Answered without the prefix (measured for {"Fn::Sub": "${Param${Item}}"}).
_SUB_VARIABLE_ERROR = ("Template error: variable names in Fn::Sub syntax must contain "
                       "only alphanumeric characters, underscores, periods, and colons")

_ALPHANUMERIC = re.compile(r"[A-Za-z0-9]+")
_SUB_VARIABLE = re.compile(r"\$\{([^}]*)\}")

# "only supported in the Resources, Conditions, and Outputs sections".
_SECTIONS = ("Conditions", "Resources", "Outputs")

# ${Identifier} and &{Identifier}; ${!Literal} is not a placeholder.
_PLACEHOLDER = re.compile(r"([$&])\{([^!}][^}]*)\}")

# Functions only the provisioned stack answers.
_DEFERRED = ("Fn::GetAtt", "Fn::ImportValue", "Fn::GetAZs", "Fn::Cidr",
             "Fn::GetStackOutput")

_LIST_PARAMETER_TYPES = ("AWS::SSM::Parameter::Value<List<String>>",
                         "AWS::SSM::Parameter::Value<CommaDelimitedList>")


class LanguageExtensionsError(ValueError):
    """A template the transform refuses; the message is answered as is."""


class _Unresolved(Exception):
    """A value the transform cannot compute from the template alone."""


_REMOVE = object()  # a Ref to AWS::NoValue: the member is dropped


def _fail(message):
    raise LanguageExtensionsError(_ERROR + message)


class _Context:
    """The mappings, the parameter values as literals and the list parameters
    the transform resolves against."""

    def __init__(self, template, parameters):
        self.mappings = template.get("Mappings") or {}
        self.parameters = parameters
        self.declared = template.get("Parameters") or {}
        self.section = None
        self.lists = {
            name for name, defn in self.declared.items()
            if isinstance(defn, dict)
            and (str(defn.get("Type", "")).startswith(("CommaDelimitedList", "List<"))
                 or defn.get("Type") in _LIST_PARAMETER_TYPES)
        }


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

def apply(template, parameters):
    """The template with the transform applied, ``parameters`` being the
    ``{name: value}`` map of the operation. Raises ``LanguageExtensionsError``."""
    _refuse_outside_sections(template)
    _refuse_duplicate_loops(template)
    context = _Context(template, parameters)
    processed = dict(template)
    for section in _SECTIONS:
        if section in template:
            context.section = section
            processed[section] = _expand(template[section], {}, context, ids=True)
    _resolve_member_conditions(processed, context)
    return processed


def _resolve_member_conditions(processed, context):
    """The ``Condition`` member of a resource or an output, which takes a
    condition name only. An intrinsic there resolves to the name (measured for
    ``{"Fn::Sub": "Is${Item}"}``); a name no condition carries is refused with
    the account's sentence (measured for a plain ``"Is${Item}"``), since the
    engine would read an unknown condition as true."""
    conditions = processed.get("Conditions") or {}
    for section in ("Resources", "Outputs"):
        for definition in (processed.get(section) or {}).values():
            if not isinstance(definition, dict) or "Condition" not in definition:
                continue
            name = definition["Condition"]
            if not isinstance(name, str):
                try:
                    resolved = _value(name, {}, context)
                except _Unresolved:
                    continue
                if not isinstance(resolved, str):
                    continue
                definition["Condition"] = name = resolved
            if name not in conditions:
                _fail(f"Key {name} is missing in the map.")


def refuse_undeclared(template):
    """Refuse a function of the transform in a template that does not declare
    it. An ``Fn::ForEach`` in ``Resources`` answers the account's sentence
    (measured from CreateStack and ValidateTemplate); the rest keep this
    emulator's wording."""
    for section in _SECTIONS:
        used, pointer = _first_function(template.get(section), f"/{section}")
        if not used:
            continue
        if used == "Fn::ForEach" and section == "Resources":
            raise LanguageExtensionsError(
                f"Template format error: [{pointer}] resource definition is malformed")
        _fail(f"{used} requires the {TRANSFORM} transform, which the template "
              "does not declare.")


def refuse_embedded(template):
    """"You can't use AWS::LanguageExtensions as a transform embedded in any
    other template section"."""
    stack = [value for section, value in template.items() if section != "Transform"]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            spec = node.get("Fn::Transform")
            if spec == TRANSFORM or (isinstance(spec, dict) and spec.get("Name") == TRANSFORM):
                _fail("the transform is declared at the top level of a template only.")
            stack.extend(node.values())
        elif isinstance(node, list):
            stack.extend(node)


def _first_function(node, path=""):
    """The first function of the transform in ``node`` and its JSON pointer,
    or ``(None, path)``."""
    if isinstance(node, dict):
        for key, value in node.items():
            if key.startswith(_FOREACH):
                return "Fn::ForEach", f"{path}/{key}"
            if key in (_LENGTH, _TO_JSON):
                return key, f"{path}/{key}"
            found = _first_function(value, f"{path}/{key}")
            if found[0]:
                return found
    elif isinstance(node, list):
        for index, value in enumerate(node):
            found = _first_function(value, f"{path}/{index}")
            if found[0]:
                return found
    return None, path


def _refuse_outside_sections(template):
    for section, value in template.items():
        if section in _SECTIONS or section == "Transform":
            continue
        used, _pointer = _first_function(value, f"/{section}")
        if used:
            _fail(f"{used} is not supported in the {section} section. The functions of "
                  f"{TRANSFORM} are supported in the Resources, Conditions and Outputs "
                  "sections.")


def _refuse_duplicate_loops(template):
    """A loop name "must be unique within the template and can't conflict with
    any logical ID values in the Resources section". It is read off the
    template as written, so a nested loop is one declaration however often the
    outer loop repeats it."""
    resources = template.get("Resources") or {}
    seen = set()

    def walk(node):
        if isinstance(node, list):
            for item in node:
                walk(item)
        elif isinstance(node, dict):
            for key, value in node.items():
                name = key[len(_FOREACH):] if key.startswith(_FOREACH) else ""
                if name:
                    if name in seen:
                        _fail(f"The loop name {name} is used more than once. A loop name "
                              "must be unique within the template.")
                    if name in resources:
                        _fail(f"The loop name {name} is the logical id of a resource. A "
                              "loop name cannot conflict with a logical id in the "
                              "Resources section.")
                    seen.add(name)
                walk(value)

    for section in _SECTIONS:
        walk(template.get(section))


# ---------------------------------------------------------------------------
# The structural pass
# ---------------------------------------------------------------------------

def _expand(node, bindings, context, ids=False):
    """``node`` with its loops expanded, the identifiers substituted where they
    are and the transform's functions resolved. ``ids`` marks a mapping whose
    keys are logical ids or output keys (a section, or the fragment of a loop
    directly in one)."""
    if isinstance(node, str):
        return node
    if isinstance(node, list):
        expanded = [_expand(item, bindings, context) for item in node]
        return [item for item in expanded if item is not _REMOVE]
    if not isinstance(node, dict):
        return node

    if len(node) == 1:
        key, argument = next(iter(node.items()))
        if key in (_LENGTH, _TO_JSON):
            try:
                return _value(node, bindings, context)
            except _Unresolved:
                if key == _LENGTH:
                    _fail(_LENGTH_UNRESOLVED)
                # Documented for Fn::GetAtt, Fn::If, Ref to a pseudo parameter
                # (measured for the first and the last): left to the engine.
                return {_TO_JSON: _partial(argument, bindings, context)}
        if key == "Fn::FindInMap":
            return _find_in_map_node(argument, bindings, context)
        if key == "Fn::Sub":
            return {"Fn::Sub": _expand_sub(argument, bindings, context)}
        if key == "Ref":
            if isinstance(argument, str) and argument in bindings:
                return bindings[argument]
            return _reference(_expand(argument, bindings, context))
        if key == "Fn::GetAtt":
            return {"Fn::GetAtt": _get_att(_expand(argument, bindings, context))}

    expanded = {}
    for key, value in node.items():
        if key.startswith(_FOREACH):
            for produced_key, produced_value in _for_each(key, value, bindings, context, ids):
                if produced_key in expanded:
                    _fail(f"{key} produces {produced_key}, which the template already "
                          "declares.")
                if produced_value is not _REMOVE:
                    expanded[produced_key] = produced_value
            continue
        resolved = _expand(value, bindings, context)
        if resolved is not _REMOVE:
            expanded[key] = resolved
    return expanded


def _substitute(text, bindings, forms="$&"):
    """The placeholders of ``text`` in ``forms`` that a loop in scope binds,
    replaced by the item; ``&`` drops its non-alphanumeric characters."""
    if not bindings:
        return text

    def replace(match):
        form, name = match.group(1), match.group(2)
        if form not in forms or name not in bindings:
            return match.group(0)
        item = bindings[name]
        return re.sub(r"[^0-9A-Za-z]", "", item) if form == "&" else item

    return _PLACEHOLDER.sub(replace, text)


def _sub_text(text, bindings):
    """The string of an ``Fn::Sub`` with ``${Id}`` filled in (``&{Id}`` stays,
    measured), refused for a variable nested in a variable name (measured);
    every other name is the engine's."""
    substituted = _substitute(text, bindings, "$")
    if any("${" in match.group(1) for match in _SUB_VARIABLE.finditer(substituted)):
        raise LanguageExtensionsError(_SUB_VARIABLE_ERROR)
    return substituted


def _reference(name):
    if isinstance(name, str):
        return {"Ref": name}
    literal = _literal_sub(name)
    if literal is None:
        _fail("Ref takes the name of a parameter or a resource here. An intrinsic "
              "function inside Ref is supported by the transform, but not by this "
              "implementation, unless a loop resolves it to a name.")
    return {"Ref": literal}


def _get_att(arguments):
    if isinstance(arguments, str):
        return arguments
    if not isinstance(arguments, list):
        _fail("Fn::GetAtt takes the logical id of a resource and the name of an "
              "attribute.")
    resolved = []
    for argument in arguments:
        literal = argument if isinstance(argument, str) else _literal_sub(argument)
        if literal is None:
            _fail("Fn::GetAtt takes the logical id of a resource and the name of an "
                  "attribute here. An intrinsic function inside Fn::GetAtt is supported "
                  "by the transform, but not by this implementation, unless a loop "
                  "resolves it to a name.")
        resolved.append(literal)
    return resolved


def _literal_sub(node):
    """The constant string of an ``Fn::Sub`` a loop filled in, or None: the
    reference names a copy as ``{"Ref": {"Fn::Sub": "Subnet${Id}"}}``."""
    if not isinstance(node, dict) or len(node) != 1:
        return None
    value = node.get("Fn::Sub")
    if isinstance(value, str) and "${" not in value:
        return value
    return None


# ---------------------------------------------------------------------------
# Fn::ForEach
# ---------------------------------------------------------------------------

def _for_each(key, spec, bindings, context, ids):
    """The members one ``Fn::ForEach`` produces, in collection order."""
    if not key[len(_FOREACH):]:
        _fail(f"{key} has no loop name.")
    if not isinstance(spec, list) or len(spec) != 3:
        _fail(f"{key} takes an identifier, a collection and a fragment.")
    identifier, collection, fragment = spec
    if not isinstance(identifier, str) or not identifier:
        _fail(f"The identifier of {key} must be a string.")
    if not isinstance(fragment, dict) or not fragment:
        _fail(f"The fragment of {key} must be an object.")
    for output_key in fragment:
        if output_key.startswith(_FOREACH):
            continue
        if identifier not in [match.group(2) for match in _PLACEHOLDER.finditer(output_key)]:
            _fail(f"The key {output_key} of {key} does not contain ${{{identifier}}} or "
                  f"&{{{identifier}}}. Every key a loop produces has to carry its "
                  "identifier, or the iterations would collide.")

    produced = []
    for item in _collection(collection, key, bindings, context):
        scoped = dict(bindings, **{identifier: item})
        for fragment_key, value in fragment.items():
            if fragment_key.startswith(_FOREACH):
                produced.extend(_for_each(fragment_key, value, scoped, context, ids))
                continue
            produced_key = _substitute(fragment_key, scoped)
            if ids and not _ALPHANUMERIC.fullmatch(produced_key):
                # Measured for an output key; the logical id wording is ours.
                noun = "OutputKey" if context.section == "Outputs" else "LogicalId"
                _fail(f"{noun} '{produced_key}' should be alphanumeric")
            produced.append((produced_key, _expand(value, scoped, context)))
    return produced


def _collection(collection, key, bindings, context):
    """The items of a loop: an array, or an intrinsic that answers one. A
    string answers the layout error, anything else that is not an array the
    missing collection (both measured)."""
    if isinstance(collection, str):
        _fail(_LAYOUT_ERROR)
    try:
        items = _value(collection, bindings, context)
    except _Unresolved:
        _fail(_NO_COLLECTION)
    if not isinstance(items, list):
        _fail(_NO_COLLECTION)
    for item in items:
        if isinstance(item, bool) or not isinstance(item, (str, int, float)):
            _fail(f"The collection of {key} holds a value that is not a string.")
    return [str(item) for item in items]


# ---------------------------------------------------------------------------
# Transform-time values
# ---------------------------------------------------------------------------

def _is_function(key):
    """A one-member object is a call only when the member names a function,
    so ``{"config": {...}}`` is an object Fn::ToJsonString serialises."""
    return isinstance(key, str) and (key.startswith("Fn::") or key in ("Ref", "Condition"))


def _value(node, bindings, context):
    """``node`` as a value computed from the template alone; ``_Unresolved``
    for anything that needs the provisioned stack."""
    if isinstance(node, str):
        return node
    if isinstance(node, list):
        values = [_value(item, bindings, context) for item in node]
        return [value for value in values if value is not _REMOVE]
    if not isinstance(node, dict):
        return node
    if len(node) != 1 or not _is_function(next(iter(node))):
        members = ((key, _value(value, bindings, context)) for key, value in node.items())
        return {key: value for key, value in members if value is not _REMOVE}

    function, argument = next(iter(node.items()))
    if function in _DEFERRED or function.startswith(_FOREACH):
        raise _Unresolved(function)
    if function == "Ref":
        return _ref_value(argument, bindings, context)
    if function == "Fn::FindInMap":
        return _find_in_map(argument, bindings, context)
    if function == _LENGTH:
        items = _value(argument, bindings, context)
        if not isinstance(items, list):
            raise _Unresolved(function)
        return len(items)
    if function == _TO_JSON:
        # Compact separators, a list parameter as an array (measured).
        return json.dumps(_value(argument, bindings, context), separators=(",", ":"))
    if function in ("Fn::Split", "Fn::Select", "Fn::Join"):
        if not isinstance(argument, list) or len(argument) != 2:
            raise _Unresolved(function)
        return _two_argument_value(function, argument, bindings, context)
    if function == "Fn::Sub":
        return _sub_value(argument, bindings, context)
    raise _Unresolved(function)


def _partial(node, bindings, context):
    """As much of ``node`` as the transform resolves, the rest expanded for
    the engine."""
    try:
        return _value(node, bindings, context)
    except _Unresolved:
        pass
    if isinstance(node, list):
        return [_partial(item, bindings, context) for item in node]
    if isinstance(node, dict) and not (len(node) == 1 and _is_function(next(iter(node)))):
        return {key: _partial(value, bindings, context) for key, value in node.items()}
    return _expand(node, bindings, context)


def _expand_sub(argument, bindings, context):
    """The argument of an ``Fn::Sub`` left for the stack, with the loop
    identifiers filled in and the variables of the map form expanded."""
    if isinstance(argument, str):
        return _sub_text(argument, bindings)
    if (isinstance(argument, list) and len(argument) == 2
            and isinstance(argument[0], str) and isinstance(argument[1], dict)):
        return [_sub_text(argument[0], bindings),
                {key: _expand(value, bindings, context)
                 for key, value in argument[1].items()}]
    return _expand(argument, bindings, context)


def _sub_value(argument, bindings, context):
    """``Fn::Sub`` as a transform-time value. An account resolves the map form
    during the transform, so one can be a member of a collection (measured).
    A variable left over is the stack's, so the value is unresolved."""
    if isinstance(argument, str):
        text, variables = argument, {}
    elif (isinstance(argument, list) and len(argument) == 2
          and isinstance(argument[0], str) and isinstance(argument[1], dict)):
        text, variables = argument
    else:
        raise _Unresolved("Fn::Sub")
    substituted = _sub_text(text, bindings)
    for name, value in variables.items():
        resolved = _value(value, bindings, context)
        if resolved is _REMOVE or isinstance(resolved, (list, dict)):
            raise _Unresolved("Fn::Sub")
        substituted = substituted.replace("${" + name + "}", str(resolved))
    if "${" in substituted.replace("${!", ""):
        raise _Unresolved("Fn::Sub")
    return substituted.replace("${!", "${")


def _two_argument_value(function, argument, bindings, context):
    first, second = argument
    if function == "Fn::Split":
        source = _value(second, bindings, context)
        if not isinstance(first, str) or not isinstance(source, str):
            raise _Unresolved(function)
        return source.split(first)
    items = _value(second, bindings, context)
    if not isinstance(items, list):
        raise _Unresolved(function)
    if function == "Fn::Join":
        return str(first).join(str(item) for item in items)
    index = _value(first, bindings, context)
    try:
        index = int(index)
    except (TypeError, ValueError):
        raise _Unresolved(function) from None
    if not 0 <= index < len(items):
        # Measured for index -1.
        _fail(f"Fn::Select cannot select nonexistent value at index {index}")
    return items[index]


def _ref_value(name, bindings, context):
    """A ``Ref`` as a value: a loop identifier or a parameter; a list parameter
    answers its members."""
    if not isinstance(name, str):
        name = _value(name, bindings, context)
    if name in bindings:
        return bindings[name]
    if name == "AWS::NoValue":
        return _REMOVE
    if name in context.parameters:
        value = context.parameters[name]
        if name in context.lists:
            return [part.strip() for part in str(value).split(",")]
        return value
    raise _Unresolved(f"Ref {name}")


def _find_in_map(arguments, bindings, context):
    """``Fn::FindInMap`` with its ``DefaultValue``, through the engine's lookup.
    Resolving it here is what puts a per-item value into a place no intrinsic
    is resolved in, such as the ``Condition`` of a copy."""
    from .engine import find_in_map  # the engine imports this module

    if not isinstance(arguments, list) or len(arguments) < 3:
        raise _Unresolved("Fn::FindInMap")
    try:
        return find_in_map(arguments, context.mappings,
                           lambda node: _value(node, bindings, context))
    except LanguageExtensionsError:
        raise
    except ValueError:
        raise _Unresolved("Fn::FindInMap") from None


def _find_in_map_node(arguments, bindings, context):
    """The value when the transform can compute it, otherwise the node with
    its loops expanded, which ``engine.find_in_map`` resolves later."""
    try:
        return _find_in_map(arguments, bindings, context)
    except _Unresolved:
        return {"Fn::FindInMap": _expand(arguments, bindings, context)}
