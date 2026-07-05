# TODOs

* [x] Properly Dockerize everything
* [ ] Unify env variables/arg names
* [ ] Convert this to use a continuous port range
* [ ] Properly document audio stream formats between all components. Diagram!
* [ ] Harness clean up long spaces and several newlines in a row properly
* [ ] Use icecast OOB API to update metadata on stream after starting
* [ ] Crash-tolerance for:
  * [ ] text generator (crash, pick up conversation again?)
  * [ ] tts generator (use rpeek instead of rpop?)
* [ ] Buttons to start generation remotely (clears last history, for now, until crash tolerance), and button to stop generation
* [ ] Selector for prompt and voice on webpage
* [ ] Send transcript to webpage
* [ ] auto-killer that queries listeners at icecast:8069/status-json.xsl, and stops generation if zero listeners for 1 minute and at least 5 minutes since stream started
* [ ] Bake static content into image
* [ ] Find any "24000" sample rates and replace with constants, and then with a redis key