### Identifying links between nodes

When a slot’s range resolves to another LinkML class, the compiler will treat it as a relationship rather than a normal property. The name of the link will be derived from the source slot name, the `target_type` from the slot’s range and the required status directly from `slot.required` (from SchemaView)

Multiplicity is determined from the [SlotDefintion](https://linkml.io/linkml/code/metamodel.html#linkml_runtime.linkml_model.meta.SlotDefinition) (from SchemaView).  However, LinkML only requires that one side of  the relationship between two classes is described whereas Gen3 describes both ends of the relationship between two nodes.

Below is how we can derive the Gen3 multiplicity if both sides are described in LinkML.

| Sourceslot maximum | Inverse slot maximum | Gen3 multiplicity |
| :---- | :---- | :---- |
| At most one | At most one | one\_to\_one |
| At most one | Many/unbounded | many\_to\_one |
| Many/unbounded  | At most one | one\_to\_many |
| Many/unbounded | Many/unbounded | many\_to\_many |

###

If we only have one side of the relationship defined in LinkML, then we can still derive the Gen3 multiplicity with sensible assumptions:

| Effective source slot | Source cardinality | Default Gen3 multiplicity |
| :---- | :---- | :---- |
| required: false, multivalued: false | 0..1 | one\_to\_one |
| required: true, multivalued: false | 1 | one\_to\_one |
| required: false, multivalued: true | 0..\* | one\_to\_many |
| required: true, multivalued: true | 1..\* | one\_to\_many |
| multivalued: true, maximum\_cardinality: 1 | 0..1 or 1 | one\_to\_one |
| multivalued: true, maximum\_cardinality: n, where n \> 1 | 0..n or 1..n | many\_to\_many |
| multivalued: true, exact\_cardinality: 1 | exactly 1 | one\_to\_one |
| multivalued: true, exact\_cardinality: n, where n \> 1 | exactly n | many\_to\_many |
