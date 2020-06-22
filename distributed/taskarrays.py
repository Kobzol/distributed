import uuid

import cloudpickle

from distributed.worker import dumps_function, warn_dumps


class Expression:

    def to_arg_def(self, names):
        raise NotImplementedError

    def dependencies(self):
        return frozenset()


class IntExpression(Expression):

    def to_arg_def(self, _names):
        return {"int": self.to_inline_def()}

    def to_inline_def(self):
        raise NotImplementedError

    def __floordiv__(self, other):
        other = to_int_expr(other)
        return IntBinOp("div", self, other)

    def __mod__(self, other):
        other = to_int_expr(other)
        return IntBinOp("mod", self, other)

    def __mul__(self, other):
        other = to_int_expr(other)
        return IntBinOp("mul", self, other)

    def __add__(self, other):
        other = to_int_expr(other)
        return IntBinOp("add", self, other)


class Index(IntExpression):

    def to_inline_def(self):
        return "index"


class IntConstant(IntExpression):

    def __init__(self, value):
        self.value = value

    def to_inline_def(self):
        return {"const": self.value}


class IntBinOp(IntExpression):

    def __init__(self, op, arg1, arg2):
        self.op = op
        self.arg1 = arg1
        self.arg2 = arg2

    def to_inline_def(self):
        return {self.op: [self.arg1.to_inline_def(), self.arg2.to_inline_def()]}


def to_int_expr(value):
    if isinstance(value, IntExpression):
        return value
    if isinstance(value, int):
        return IntConstant(value)


def to_int_expr_checked(value, if_none=None):
    if value is None and if_none is not None:
        return IntConstant(if_none)
    result = to_int_expr(value)
    if result is None:
        raise Exception("'{!r}' is not integer expression".format(value))
    return result


index = Index()


class GetItem(Expression):

    def __init__(self, task_array, index):
        self.task_array = task_array
        self.index = index

    def dependencies(self):
        return frozenset((self.task_array,))

    def to_arg_def(self, names):
        return {"task-array": [names[self.task_array], {"get-item": self.index.to_inline_def()}]}


class Slice(Expression):

    def __init__(self, task_array, start, stop, step):
        self.task_array = task_array
        self.start = start
        self.stop = stop
        self.step = step

    def dependencies(self):
        return frozenset((self.task_array,))

    def to_arg_def(self, names):
        return {"task-array": [names[self.task_array],
                               {"slice": [self.start.to_inline_def(),
                                          self.stop.to_inline_def(),
                                          self.step.to_inline_def()]}]}


class TaskRef:
    def __init__(self, key):
        self.key = key

    def dependencies(self):
        return frozenset()

    def to_arg_def(self, names):
        return {
            "task": self.key
        }


class TaskArrayPart:

    def __init__(self, size, function, arg_exprs, kwargs=None):
        self.size = size
        self.function = function
        self.arg_exprs = arg_exprs
        self.kwargs = kwargs

    def dependencies(self):
        result = frozenset()
        for a in self.arg_exprs:
            result |= get_arg_dependencies(a)
        return result

    def to_dict(self, names):
        data = {
            "size": self.size,
            "function": dumps_function(self.function),
            "args": [make_arg_def(a, names) for a in self.arg_exprs],  # TODO,
        }
        if self.kwargs is not None:
            data["kwargs"] = warn_dumps(self.kwargs)
        return data

    def __getitem__(self, item):
        return GetItem(self, to_int_expr(item))


def get_arg_dependencies(value):
    if isinstance(value, TaskArray):
        return frozenset((value,))
    if isinstance(value, Expression):
        return value.dependencies()
    return frozenset()


def make_arg_def(value, names):
    if isinstance(value, TaskArray):
        return {"task-array": [names[value], "all"]}
    if isinstance(value, (Expression, TaskRef)):
        return value.to_arg_def(names)
    if isinstance(value, bool):
        return {"bool": value}
    if isinstance(value, int):
        return IntConstant(value).to_arg_def(names)
    return {"serialized": cloudpickle.dumps(value)}


class TaskArray:
    @staticmethod
    def from_parts(parts):
        assert len(parts) > 0
        part = parts[0]
        array = TaskArray(part.size, part.function, part.arg_exprs, part.kwargs)
        for part in parts[1:]:
            array.add_part(part)
        return array

    @staticmethod
    def from_constant_list(items):
        assert len(items) > 0

        def make_part(item):
            return TaskArrayPart(1, lambda x: x, [item])

        part = make_part(items[0])
        array = TaskArray(part.size, part.function, part.arg_exprs, part.kwargs)
        for item in items[1:]:
            array.add_part(make_part(item))
        return array

    def __init__(self, size, function, arg_exprs, kwargs=None):
        self.parts = [TaskArrayPart(size, function, arg_exprs, kwargs)]
        self.size = size
        self.id = uuid.uuid4().hex

    def dependencies(self):
        result = frozenset()
        for part in self.parts:
            result |= part.dependencies()
        return result

    def add_part(self, part):
        self.parts.append(part)
        self.size += part.size

    def __getitem__(self, item):
        if isinstance(item, slice):
            start = to_int_expr_checked(item.start, if_none=0)
            stop = to_int_expr_checked(item.stop, if_none=self.size)
            step = to_int_expr_checked(item.step, if_none=1)
            return Slice(self, start, stop, step)
        expr = to_int_expr(item)
        if expr is not None:
            return GetItem(self, expr)
        raise Exception("Invalid slice expression")
