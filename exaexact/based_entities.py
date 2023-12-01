from pyodata.v2.model import EntityType

class BasedEntityType(EntityType):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._base  = False
        self._based = False
        
    @classmethod
    def from_etree(cls, type_node, config):
        etype = super(BasedEntityType, cls).from_etree(type_node, config)
        etype._base = type_node.attrib.get('BaseType')
        return etype
    
    def rebase(self, schema):
        if not self._based:
            if self._base:
                decl, typ = self._base.rsplit('.', 1)
                base = schema._decls[decl].entity_types[typ]
                self._key = self._key or base._key
                self._properties     = {**base._properties, **self._properties}
                self._nav_properties = {**base._nav_properties, **self._nav_properties}
        self._based = True

def based_entitites_patch():
    from unittest.mock import patch
    original = super(BasedEntityType, BasedEntityType).from_etree
    def _wrapped(*_args, **_kwargs):
        with patch.object(EntityType, 'from_etree', original):
            return BasedEntityType.from_etree(*_args, **_kwargs)
    return patch.object(EntityType, 'from_etree', _wrapped)
    
def rebase_schema(schema):
    for etype in schema.entity_types:
        if isinstance(etype, BasedEntityType):
            etype.rebase(schema)