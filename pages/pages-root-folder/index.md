---
#
# Use the widgets beneath and the content will be
# inserted automagically in the webpage. To make
# this work, you have to use › layout: frontpage
#
layout: frontpage
header:
  title: Genie
  image_fullwidth: black-polygon.jpg
  caption: Source
  caption_url: http://wall-papers.info/wallpapers/background-black-and-red.html
widget1:
  title: About
  text: High level information about Genie
  image: /images/logo.png
  url: /info/
widget2:
  title: Releases
  url: /releases/
  image: /images/matrix.jpg
  text: Find binaries and information about various Genie releases
widget3:
  title: Meetup
  text: NetflixOSS Meetup Season 3 Episode 1
  url: /presentations/OSSMeetupSeason3Episode1.html
  video:
    image: http://img.youtube.com/vi/hi7BDAtjfKY/hqdefault.jpg
    src: https://www.youtube.com/embed/hi7BDAtjfKY?start=956
#
# Use the call for action to show a button on the frontpage
#
# To make internal links, just use a permalink like this
# url: /getting-started/
#
# To style the button in different colors, use no value
# to use the main color or success, alert or secondary.
# To change colors see sass/_01_settings_colors.scss
#
#callforaction:
#  url: https://tinyletter.com/feeling-responsive
#  text: Inform me about new updates and features ›
#  style: alert
permalink: /
breadcrumb: false
---

{% include _improve_content.html %}
