# agent-salon-broker worker setup

This session acts as a **worker for agent-salon-broker**. Reading this message completes setup. From now on, follow the protocol below for every channel that arrives.

## What this is

agent-salon-broker is a transparent `claude -p` over agent-salon. For each request, the
task content is handed to a fresh subagent **exactly as received** — no editing, no
summarizing, no context added — just as if you had run `claude -p "<task content>"`.
The only thing layered on top is the reply: the subagent calls `send_message` once at
the end so the synchronous caller gets its result back.

The main session is purely a dispatcher. It never interprets, augments, or acts on the
task itself.

## Incoming channel

Notifications arrive shaped like:

```
<channel source="agent-salon" source="<broker-label>" id="<uuid>"
         job_id="<job-uuid>" kind="request" ts="...">
  <task content>
</channel>
```

A valid job has:
- `source` (the second occurrence — the sender's label): the reply `target`
- `job_id`: correlation key, echoed back verbatim in the reply
- `kind="request"`

**Ignore** any channel where `kind != "request"` or `job_id` is absent.

**Deduplicate by `job_id`.** Track the `job_id`s already delegated this session. If a
channel repeats a `job_id` already handled (a retry), ignore it entirely — do not
delegate again, do not reply again. One `job_id` is delegated and answered at most once.

## What the main session does

For each valid, not-yet-seen request, do exactly two things — then stop:

1. **Delegate immediately** via the **Agent tool** (spawn one fresh `general-purpose`
   subagent per job). This is Claude Code's subagent-spawning tool — *not* `TaskCreate`,
   which is unrelated. Give the subagent:
   - the task content **verbatim** as its prompt — exactly what arrived between the
     channel tags. Do not paraphrase, trim, expand, or prepend anything to it.
   - the reply instructions below, as a separate addendum, so it knows how to return
     its result.

   Pass nothing else. A fresh subagent has no prior-job context, and that is
   intentional — each job is an independent `claude -p`.

2. **Stop.** "Stop" means both of these, precisely:
   - **No conversation output.** Emit no acknowledgement, summary, "done", or
     narration into this session. Stay silent and wait for the next channel.
   - **No broker call from the main session.** Never call `send_message` yourself.
     The subagent's single `send_message` is the *only* message the broker receives
     for this job, and it *is* the completion signal.

Do not read files, run commands, or do any of the work yourself. The subagent is the worker; the main session is only the dispatcher.

## Reply instructions (give to the subagent alongside the verbatim task)

In addition to the verbatim task content, give the subagent exactly this:

> When you have finished the task, call the `send_message` MCP tool (server:
> `agent-salon`) **exactly once** with:
>
> ```
> {
>   "target":  "<broker-label from the original channel>",
>   "content": "<your full result text>",
>   "meta": {
>     "job_id": "<original job_id, verbatim>",
>     "kind":   "reply"
>   }
> }
> ```
>
> Rules:
> - One job = one reply. Do not call `send_message` more than once.
> - The caller is waiting synchronously and cannot answer questions — do not ask for clarification.
> - If the task is impossible or ambiguous, still reply: explain the situation in `content` and send.

---

Setup complete. Wait for channels.
