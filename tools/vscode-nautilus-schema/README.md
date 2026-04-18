# Nautilus Schema VS Code Extension

Language support for the [Nautilus](https://github.com/y0gm4/nautilus) schema DSL (`*.nautilus` files).

## Features

- syntax highlighting, brackets, comments, and snippets for `.nautilus`
- diagnostics powered by `nautilus-lsp`
- completions, hover, and go-to-definition
- whole-file formatting
- semantic tokens for models, enums, and composite types
- automatic `nautilus-lsp` download on first activation when no explicit path is configured
- automatic refresh of the cached `nautilus-lsp` binary when GitHub publishes a newer release

## Installation

### From a release `.vsix`

1. Download the extension from the latest GitHub release.
2. In VS Code choose `Extensions -> ... -> Install from VSIX...`.
3. On first activation the extension resolves `nautilus-lsp` in this order:
   - `nautilus.lspPath`
   - local repo debug build
   - cached downloaded binary (auto-updated when GitHub has a newer release)
   - `nautilus-lsp` on `PATH`
   - GitHub release download

### Manual binary override

```json
{
  "nautilus.lspPath": "/absolute/path/to/nautilus-lsp"
}
```

### Local development

```bash
cargo build -p nautilus-orm-lsp
cd tools/vscode-nautilus-schema
npm ci
npm run bundle
```

Then open the extension folder in VS Code and press `F5`.

## Configuration

| Setting | Default | Description |
| --- | --- | --- |
| `nautilus.lspPath` | `""` | Absolute path to the `nautilus-lsp` binary. Empty means auto-resolve, reuse the cached auto-updated binary, or download it from GitHub. |
