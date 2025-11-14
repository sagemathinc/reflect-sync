Prove a theorem that under certain conditions file corruption is not possible.

**PROPOSITION:** _Using our last-write-wins merge strategy LWW, if a file is
only edited by the user on beta, then reflect-sync will not change that file
on beta, only mirroring it to alpha. If the user switches to editing that file
on alpha after waiting for at least one additional scan on beta to complete,
then edits only on beta, then reflect-sync will not change the file on alpha,
only mirroring it to beta.  If the user edits the file on alpha before waiting
for one additional scan on beta, that edit will be overwritten._

(NOTE: we are only considering editing of a file in this proposition -- not deleting, not directories, etc.)

**Proof.** Call the file A. Suppose at time 0 we schedule to copy A from beta to
alpha, which will be done via either rsync or "cp --reflink", both of which are
atomic or fail.

If the copy fails (e.g., network problem, file changed during copy attempt,
etc.) then we do not make any change to state and nothing has happened.

If the copy succeeds then some version of A got copied from beta to alpha
atomically and successfully, but we don't know what version. It can be
completely different than what was on beta when we did the last scan. We update
metadata assuming that the copy was from the last version on A, but we mark our
metadata about A on alpha as *pending*.

As long as the metadata about A is pending, our merge function returns "no-op",
and we do not do any operations (copy or delete).

The data about A on alpha is only set to not pending once we have successfully
completed a scan on alpha including A. We also update base to the metadata of A
on alpha, when we determine it, because that was the correct base at the moment
of the copy, since we are assuming that the user is actually not actively
editing files on alpha. (If they are actively editing on both sides, we fully expect the file to randomly flip-flop in an undefined way, and thus make no guarantees.)

Now assume the data is no longer pending so we are comparing the data we have
about A on beta, alpha and in base. Under our assumption that the user is not
actively editing files on alpha, the metadata in base and on alpha is the same,
so we select alpha in doing our three way merge, and correctly copy A to alpha
again.

---

What happens when the user stops changing A on beta and starts making changes to
A on alpha. If they change A *while* the metadata is pending, then that one
change will get overwritten by the last value on beta... basically beta has a
little momentum. If instead they wait until after the metadata is updated (so no
longer pending), then their change will be correctly replicated to beta. This is
very acceptable, given our goals.

☐

TODO

CLARIFICATION: [...]

Prop: Same as above, but the user may also delete the file on beta.  Then
the conclusion is the same.

Proof. (??? not sure how deletes work yet)

☐


