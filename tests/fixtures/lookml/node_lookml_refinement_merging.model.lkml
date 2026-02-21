# Source: fabio-looker/node-lookml-parser (MIT License)
# File: test-projects/.014-extensions-refinements-merging/on-the-merge.model.lkml
# Tests: deep merge in refinements, +view syntax

# Objects within an object should be merged deeply
view: deep_merging {
	dimension: dim { sql: ${TABLE}.dim ;; }
	}

view: +deep_merging {
	dimension: dim { label: "My Dim" }
	}

# array example?

# filters/hashmap example

# SUPER example?