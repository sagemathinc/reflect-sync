Prove a theorem that under certain conditions file corruption is not possible.

**THEOREM:** Using our last-write-wins merge strategy, if a subtree is only modified by the user on beta, then reflect will never copy any files from
alpha to beta and will never delete any files on beta.

Proof.  We focus on the case of one single file A.

Suppose at logical time 0 we schedule to copy A from
beta to alpha. 

If the copy fails (e.g., network problem, file changed
during copy, etc.) then we do not make any change to
state and nothing has happened.

If the copy succeeds then rsync guarantees that some
version of A got copied from beta to alpha atomically
and successfully, but we don't know what version. The file
could have changed from when we last compute metadata
or a hash.  We mark our metadata about A on alpha
as pending.

As long as the metadata about A is pending, we do not
do any operations (copy or delete).

The data about A on alpha is only set to not pending once
we have successfully completed a scan on alpha including A.
We also update base to the metadata of A on alpha, when
we determine it, because that was the correct base at
the moment of the copy (the user is actually not actively
editing files on alpha).

Now assume the data is no longer pending so we are comparing 
the data we have about A on beta, alpha and in base.  Under 
our assumption that the user is not actively editing files 
on alpha, the metadata in base and on alpha is the same,
so we select alpha in doing our three way merge, and correctly
copy A to alpha again. 

If we delete A from beta there is no pending.  That case is easy.

So I think this result is pretty obvious with this notion of pending.

---

What happens when the user stops changing A on beta and starts
making changes to A on alpha. If they change A *while* the metadata
is pending, then that one change will get overwritten by the last
value on beta... basically beta has a little momentum.  If instead
they wait until after the metadata is updated (so no longer pending),
then their change will be correctly replicated to beta.
This is very acceptable, given our goals.





