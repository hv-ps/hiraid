# Work in progress: hiraid 1.0.03
hiraid is a Python package for communicating with Hitachi Enterprise storage arrays.

It is capable of communicating through either raidcom or cmrest and can be extended to utilise other communication methods.

Each command / cmrest output is stored in a logical structure beneath storageobject.views, this view can be saved as a cache file and reloaded onto the raid object in a subsequent script.

For each api capability ( raidcom / cmrest ) a parser exists in order to serialise the data into dictionaries which are then passed to a views class to produce the default stored view. Customviews can be used to arrange data how you need it or even override the default views.

The raidcom piece is well underway and has already been expanded several times as needs have risen.

CMrest is fairly barebones but ready to be expanded; the mechanism for instantiating raid objects which communicate through cmrest is in place and is demonstrated under the scripts directory ( /usr/bin/local/raid* after installation ).

## Long term goal
Normalising the data in order to achieve standard views of the storage no matter which api is used would allow for the highest degree of flexibility and is the current long term aspiration for this utility.

### Install
> pip3 install git+https://github.com/hv-ps/hiraid.git

In order for this to work you will need to create a personal access token and authorise the personal access token for use with SAML single sign-on: 

https://docs.github.com/en/free-pro-team@latest/github/authenticating-to-github/creating-a-personal-access-token 
https://docs.github.com/en/free-pro-team@latest/github/authenticating-to-github/authorizing-a-personal-access-token-for-use-with-saml-single-sign-on


