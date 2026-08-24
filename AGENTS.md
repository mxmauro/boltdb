# Go Agent Notes

## Scope
- These instructions apply to the whole repository.
- Treat this as a Go codebase first. Preserve the current repository structure instead of reshaping files or folders unless the task requires it.
- Do not touch unrelated user changes. The repository may be dirty.

## Working principles
- Think before coding. State assumptions explicitly, surface ambiguity or tradeoffs instead of picking silently, and ask for clarification when requirements or nearby code are unclear.
- Simplicity first. Make the smallest change that solves the task. Do not add speculative abstractions, configurability, or features that were not requested.
- Surgical changes. Keep diffs limited to the requested behavior, tests, and directly required documentation. Do not reformat, rename, or rewrite unrelated code. Remove only imports, variables, or helpers that your change made unused.
- Goal-driven execution. Define the verification target before editing, prefer focused tests or checks that prove the changed behavior, and broaden revalidation only as the change scope requires.

## File format
- Use LF line endings for text files unless the repository explicitly requires something else for a specific path.
- Use UTF-8 or plain ASCII. Do not introduce a different encoding unless the file already requires it.
- Keep trailing whitespace out of handwritten files, except where Markdown intentionally uses trailing spaces for formatting.
- When possible, keep line length at or below 140 characters unless the surrounding code clearly uses a different convention.
- Preserve the existing spacing and blank-line rhythm in touched files instead of normalizing whole files.

## Go coding style
- Follow standard Go style and idioms as enforced by `gofmt`. This matches the default formatting behavior used by JetBrains GoLand for Go source files.
- Apply formatting only to files you actually changed. Do not run repository-wide formatting or cleanup passes for a targeted change.
- Match the surrounding naming, comment, and API style instead of renaming identifiers for taste.
- Prefer small, readable functions and explicit control flow over clever abstractions.
- Keep comments sparse and useful. Explain intent, invariants, or concurrency constraints, not obvious statements.
- For new files or newly added code, follow the same local style rules instead of introducing a different organization pattern.
- Preserve separator comments in the established style when they already help structure a file, for example:

```text
//------------------------------------------------------------------------------
```

- Keep import blocks aligned with the surrounding file. When imports are grouped, preserve the existing grouping order:
- Go standard library imports first
- third-party or vendor imports second
- project-local imports last
- In new files, or when adding declarations to an existing file, keep exported types and functions near the top and place private helpers lower in the file unless the surrounding file already uses a different established structure.
- Preserve exported API behavior unless the task explicitly requires an API change.

## Functional and security expectations
- Prioritize correctness first, especially for synchronization, cancellation, atomics, lifecycle transitions, I/O boundaries, validation, and state management.
- Favor fail-closed behavior. On invalid state or misuse, return failure or panic consistently instead of silently continuing in a corrupted state.
- Preserve or improve validation, state checks, and safety invariants. Do not weaken them for convenience.
- Any change touching concurrency, memory ownership, shutdown, cancellation, authentication, authorization, cryptography, parsing, serialization, filesystem access, network access, or other security-relevant logic must include code revalidation before finishing.
- Revalidation is mandatory for functional changes and security-sensitive changes, not optional.
- Be careful not to expose partial state during initialization, shutdown, error handling, or concurrent access.
- Do not log secrets, credentials, tokens, keys, or other sensitive internal state.

## Tests and revalidation
- Update or extend tests when behavior changes, especially for synchronization, cancellation, lifecycle, error-path, parsing, validation, or boundary-condition logic.
- Re-run the narrowest useful validation first, then broader validation when the change affects shared behavior.
- For concurrency-sensitive Go changes, prefer running both `go test ./...` and `go test -race ./...` unless the user explicitly asks not to.
- If other project-specific validation exists, run the relevant subset for the changed area rather than skipping validation entirely.
- If full revalidation cannot be run, say so clearly and explain what remains unchecked.

## Editing rules for agents
- Keep formatting and code changes limited to the specific functions or blocks required by the task. Do not rewrite unrelated nearby code.
- Preserve line endings, indentation, and spacing exactly in untouched regions.
- If a task creates tension between style and correctness or security, prioritize correctness and security while keeping the diff as small as possible.
