class RootListMixin:
    """Mixin for Pydantic RootModel of List type that provides more pythonic access to members."""

    def __iter__(self):
        return iter(self.root)

    def __getitem__(self, item):
        return self.root[item]
