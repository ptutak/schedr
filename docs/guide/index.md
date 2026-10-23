# How schedr works

How the running system behaves, as built so far: which process does what, how a run moves
through them, and what happens when one of them fails. The **Design** section has the
reasons behind each choice and the alternatives that weren't chosen; the **Reference**
section has every configuration key, command, endpoint and metric, generated from the code.

Read [Overview](overview.md) first; the other pages each go into one part.

```{toctree}
:maxdepth: 2

overview
glossary
life-of-a-run
master
workers
parsing
failures
network
observability
deployment
```
