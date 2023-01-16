import { createTheme, CssBaseline, ThemeProvider } from "@material-ui/core";
import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App";
window.Buffer = window.Buffer || require("buffer").Buffer;

const theme = createTheme({
    palette: {
        primary: {
            main: "#EEF1DB",
            contrastText: "#000",
        },
        secondary: {
            main: "#C9C7AF",
            contrastText: "#000",
        },
        background: {
            default: "#1e1e1e",
            paper: "#1e1e1e",
        },
        divider: "#C9C7AF",
    },
});

const root = ReactDOM.createRoot(
    document.getElementById("root") as HTMLElement
);
root.render(
    <React.StrictMode>
        <ThemeProvider theme={theme}>
            <CssBaseline>
                <App />
            </CssBaseline>
        </ThemeProvider>
    </React.StrictMode>
);
