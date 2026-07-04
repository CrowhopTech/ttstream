# TODOs

* [ ] Check if the fact that rpop is blocking can improve code design: rpop returns nil (empty bytes) when queue is empty, not a blocking call. Added 0.5s sleep + continue in loop.
