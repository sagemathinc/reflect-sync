- [ ] microsync is not directory aware

- [ ] microsync breaks the test "dir→file conflict (prefer beta): currently conservative, dir on alpha remains" if you try it manually with the scheduler on

- [ ] need to not spawn child processes but call functions in scripts, since otherwise control+c doesn't terminate sync, which is annoying
