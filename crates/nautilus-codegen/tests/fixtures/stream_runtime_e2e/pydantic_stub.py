class BaseModel:
    def __init__(self, **data):
        annotations = {}
        for cls in reversed(type(self).mro()):
            annotations.update(getattr(cls, "__annotations__", {}))
        for name in annotations:
            if name == "nautilus":
                continue
            if name in data:
                value = data[name]
            else:
                value = getattr(type(self), name, None)
            setattr(self, name, value)
        for name, value in data.items():
            setattr(self, name, value)

    def model_dump(self):
        return dict(self.__dict__)

    @classmethod
    def model_rebuild(cls):
        return cls


def Field(default=None, **_kwargs):
    return default
