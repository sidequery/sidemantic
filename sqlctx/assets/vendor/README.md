# Vendored Chart Renderer Assets

These browser renderer bundles are copied from exact npm package tarballs, not runtime CDNs. They are inlined into generated standalone HTML so chart rendering does not depend on jsDelivr, esm.sh, cdn.plot.ly, unpkg, or other remote script hosts.

Re-vendor by running `npm pack <package>@<version>`, extracting the tarball, and copying the package path listed below.

| Local file | npm package | Package path | License | SHA-256 |
| --- | --- | --- | --- | --- |
| `d3-7.9.0.min.js` | `d3@7.9.0` | `dist/d3.min.js` | ISC | `f2094bbf6141b359722c4fe454eb6c4b0f0e42cc10cc7af921fc158fceb86539` |
| `observable-plot-0.6.17.umd.min.js` | `@observablehq/plot@0.6.17` | `dist/plot.umd.min.js` | ISC | `4358086467740777dd788d6b27a95cebdbaefdd50c730a3060117073bd7134cb` |
| `plotly-2.35.2.min.js` | `plotly.js-dist-min@2.35.2` | `plotly.min.js` | MIT | `6d21266ce1bd7d9e5ab4e115989c70c20de0382fd973a8f26ab58619eba4d603` |
| `vega-5.33.1.min.js` | `vega@5.33.1` | `build/vega.min.js` | BSD-3-Clause | `463f3db6a40b20e9747b4ed38f37ed0add508838f9141b1cf8366784b07b30c8` |
| `vega-embed-6.29.0.min.js` | `vega-embed@6.29.0` | `build/vega-embed.min.js` | BSD-3-Clause | `12d02acfbe3ec59ef9a37dd4822a2e04e2961b5bbb671bbe661d2221715b99da` |
| `vega-lite-5.23.0.min.js` | `vega-lite@5.23.0` | `build/vega-lite.min.js` | BSD-3-Clause | `58c27358e26f2d319cf62f45bc17a4c8362f08645001df2ec8d341eee4097c7f` |
