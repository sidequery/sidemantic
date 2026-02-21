# Source: fabio-looker/node-lookml-parser (MIT License)
# File: test-projects/013-extensions-refinements-sequencing/fun-with-extensions.model.lkml
# Tests: refinement edge cases, extension ordering, +explore syntax

view: my_view {}

# 2 simple extensions, varying only in order
explore: a_before_b { from:my_view extends: [has_label_a, has_label_b] }
explore: b_before_a { from:my_view  extends: [has_label_b, has_label_a] }

explore: has_label_a { extension: required label: "A" }
explore: has_label_b { extension: required label: "B" }

# 2 extensions, but  one is itself refined
explore: a_plus_before_b { from:my_view  extends: [has_label_a_plus, has_label_b     ] }
explore: b_before_a_plus { from:my_view  extends: [has_label_b,      has_label_a_plus] }

explore: has_label_a_plus  { extension: required label: "A" }
explore: +has_label_a_plus { label: "A+"}

# 2 extensions, but also a refinement at the root (declared earlier)
explore: +a_plus_before_b_plus_c { label: "C+" }
explore: a_plus_before_b_plus_c { from:my_view  extends: [has_label_a, has_label_b] }

# 2 extensions, but also a refinement at the root (declared later)
explore: a_plus_before_b_plus_d { from:my_view  extends: [has_label_a, has_label_b] }
explore: +a_plus_before_b_plus_d { label: "D+" }


# An explore that extends, and a refinement that extends otherwise
explore: b_plus_a {from:my_view extends: [has_label_b]}
explore: +b_plus_a {extends: [has_label_a]}

# A refined explore, but the refinement is extended by a previously applied extension
explore: b_plus_a_then_b {from:my_view extends: [has_label_b]}
explore: +b_plus_a_then_b {extends: [has_label_a, has_label_b]}

explore: a_plus_a_then_b {from:my_view extends: [has_label_a]}
explore: +a_plus_a_then_b {extends: [has_label_a, has_label_b]}
