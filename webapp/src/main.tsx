import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "./App";
import { ComponentGallery } from "./views/ComponentGallery";
import { initTheme } from "./lib/theme";
import "./index.css";

initTheme();

const root = document.getElementById("root");
if (!root) throw new Error("Missing #root element");

createRoot(root).render(
  <StrictMode>
    {window.location.pathname === "/components" ? <ComponentGallery /> : <App />}
  </StrictMode>,
);
