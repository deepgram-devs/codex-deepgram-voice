Notion page: https://app.notion.com/p/3eba615b8fd781309b4ad44954408d64

Use the build-goal skill for this run, with this prompt as the spec. Save it as GOAL.md before doing anything else. Background lives at https://app.notion.com/p/3eba615b8fd781309b4ad44954408d64, but that page's scope is out of date.

GOAL
Make Deepgram Flux the speech layer for Codex CLI voice, which OpenAI refreshed at DevDay on Sep 29, 2026. A developer speaks a task, Flux transcribes it and knows when they've finished, and Codex starts working.

REPO
This repo on branch goal/bg-4-codex-voice. The Codex source is cloned at ../codex.

STEP 1: SPIKE (2 hours max)
Answer each question with a file path in ../codex, a doc link, or command output. Write the answers plus the path you pick into DECISION.md.
1. Does the [realtime] block in Codex's config.toml accept a custom URL or endpoint? Find where realtime and transcription sessions are configured in the source.
2. What protocol and events does Codex's transcription mode expect from the server?
3. Does the push-to-talk transcription layer let you choose a speech provider? Third-party writeups say it uses Wispr Flow, so confirm that from the source.
4. Is there a hook or plugin point that can supply transcribed text to Codex?

PATHS, BEST FIRST
A. Local shim: a small local server that speaks the protocol from question 2, with Flux underneath. Flux settings: wss://api.deepgram.com/v2/listen, model flux-general-en, linear16 at 16000 Hz, 80 ms chunks. Send the EndOfTurn transcript on as the finished text. Only possible if question 1 is yes.
B. Codex fork: add Deepgram as a speech option behind a config flag, in a fork. Build and test it there, then stop and ask me before opening anything upstream.
C. Fallback: a standalone voice composer that streams the mic to Flux and sends each finished turn into Codex.

DECISION RULE
Pick the best path the spike allows. If only path B works, build it on the fork and stop before any upstream PR.

DONE WHEN
- Speaking a task in Codex CLI gets Codex working on it, with Flux doing the transcription
- Turn detection doesn't cut me off mid-thought in a 10-minute session
- The README explains setup in under 5 minutes
- social/BG-4.md has the social kit, including a 60-second shot list

GUARDRAILS
- Draft PR on our repo only. Never merge, publish, or open an upstream PR without asking me.
- Read DEEPGRAM_API_KEY from the environment and never commit a secret
- In Notion, only set BG-4's Status and add lines to its Run log
- Stop and ask me if an answer changes the scope
