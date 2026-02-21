# Source: looker-open-source/pylookml (MIT License)
# File: lookml/tests/files/basic_parsing/manifest.lkml
# Tests: comprehensive manifest with constants, dependencies, applications, visualizations

project_name: "pylookml"

local_dependency: {
  project: "ecommerce"
}
local_dependency: {
  project: "ecommerce2"
}
remote_dependency: ga360 {
  url: "https://github.com/llooker/google_ga360"
  ref: "07a20007b6876d349ccbcacccdc400f668fd8147f1"
  override_constant: example1 {
    value: "my_example_value"
  }
  override_constant: example2 {
    value: "my_example_value2"
  }
}

remote_dependency: cool2 {
  url: "https://github.com/llooker/google_ga360"
  ref: "07a20007b6876d349ccbcacccdc400f668fd8147f1"
  override_constant: example1 {
    value: "my_example_value"
  }
  override_constant: example2 {
    value: "my_example_value2"
  }
}

visualization: {
  id: "foo"
}

visualization: {
  id: "bar"
}

constant: great {
  value: "this is great"
  export: override_optional
}

constant: cool {
  value: "this is cool"
  export: override_optional
}

application: hello_world {
  label: "@{cool}"
  file: "bundle.js"
  entitlements: {
    local_storage: yes
    navigation: yes
    new_window: no
    allow_forms: yes
    allow_same_origin: yes
    core_api_methods: ["all_connections","search_folders", "run_inline_query", "me", "all_looks", "run_look"]
    external_api_urls: ["http://127.0.0.1:3000", "http://localhost:3000", "https://*.googleapis.com"]
    oauth2_urls: ["https://accounts.google.com/o/oauth2/v2/auth"]
  }
}

application: hello_other_world {
  label: "@{cool}_other"
  file: "bundle.js"
  entitlements: {
    local_storage: yes
    navigation: yes
    new_window: no
    allow_forms: yes
    allow_same_origin: yes
    core_api_methods: ["all_connections","search_folders", "run_inline_query", "me", "all_looks", "run_look"]
    external_api_urls: ["http://127.0.0.1:3000", "http://localhost:3000", "https://*.googleapis.com"]
    oauth2_urls: ["https://accounts.google.com/o/oauth2/v2/auth"]
  }
}