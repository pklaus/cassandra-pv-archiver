$(document).ready(function() {
  $("#navbar-sign-out-button").click(function(event) {
    $("#navbar-sign-out-form").submit();
    // We do not want to follow the link, because submitting the form will
    // already take care of that.
    event.preventDefault();
  });

  $("#navbar-sign-in-dropdown").on("shown.bs.dropdown", function() {
    $("#navbar-sign-in-username").focus();
  });
});
