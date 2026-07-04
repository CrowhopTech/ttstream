# TODOs

* [ ] Unify env variables/arg names
* [ ] Properly Dockerize everything
* [ ] Crash-tolerance for:
  * [ ] text generator (crash, pick up conversation again?)
  * [ ] tts generator (use rpeek instead of rpop?)
* [ ] Buttons to start generation remotely (clears last history, for now, until crash tolerance), and button to stop generation
* [ ] Selector for prompt and voice on webpage
* [ ] Send transcript to webpage
* [ ] auto-killer that queries listeners at icecast:8069/status-jsson.xsl, and stops generation if zero listeners for 1 minute and at least 5 minutes since stream started
