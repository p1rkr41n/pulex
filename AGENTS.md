# Workspace Guidelines & Instructions: Pulex (Tauri v2 Edition)

## Version Control: Jujutsu (`jj`) as Default VCS
- Jujutsu (`jj`) is the default local version control system for this project.
- `jj` executable location:
  - System Cargo Bin: `%USERPROFILE%\.cargo\bin\jj.exe`
  - Workspace Tool: `tools/jj/jj.exe`
- Workflow conventions:
  - Repositories are initialized with `jj git init --colocate` to maintain seamless dual compatibility with Git.
  - Track changes using `jj status`, `jj diff`, `jj log`, `jj describe`, and `jj commit`.
  - Always exclude large intermediate artifacts (`target/`, `node_modules/`, `dist/`, binaries) in `.gitignore`.

## Packaging & Binary De-duplication Policy
- **Zero-Duplicate Binary Storage**: Scripts (`tools/package_pulex.ps1`) must NEVER copy or duplicate the newly built binary across historical version names.
- **Pulex Releases**:
  - Releases must ONLY be output into `releases/`.
  - Canonical outputs:
    - Root: `pulex.exe` (single local development/test executable).
    - Releases: `releases/Pulex-<version>-x64-portable.exe` and `releases/Pulex-<version>-x64-portable.zip`.
- All duplicate historical binaries must remain pruned.

## Versioning Standard
- **Semantic Versioning 2.0.0**: Pulex follows standard SemVer (`MAJOR.MINOR.PATCH`, e.g., `3.0.0`, `3.0.1`) compatible with Cargo and Tauri.

## Release & Commit Identity Standard
- **Indie Release Branding**: The standard indie release author and publisher is strictly `HelloKittyXamKinLung`.
- **Commit & Release Identity**:
  - Author Name: `HelloKittyXamKinLung`
  - Author Email: `p1rkr41n@users.noreply.github.com`
- **Zero Enterprise/Org Leaks**: Never use enterprise, corporate, or organizational credentials/identities in git commits, jujutsu commits, release metadata, or packaging scripts.
- **English-Only Public Language Policy**: In all public-facing contexts (git commit messages, git tags, GitHub release titles, release notes, release assets, PRs, public issue templates, documentation, winget manifests, etc.), strictly use professional, concise English. Zero non-English text in public artifacts or releases.

