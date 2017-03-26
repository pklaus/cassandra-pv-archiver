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

/*
   Encodes a string so that it can safely be used as part of a URI.

   This is very similar to encodeURIComponent, but encodes a few more special
   characters (".", "!", "~", "*", "'", "(", ")"). This means that only the
   ASCII characters A to Z and a to z, the digits 0 to 9, and the two special
   characters "-" and "_" are not encoded. In addition to that, this encoder
   uses "~" as the escape character, while encodeURIComponent uses "%".
*/
encodeURIComponentCustom = function(str) {
  // First, we run the string through encodeURIComponent. This will convert
  // everything to UTF-8 and already remove most special characters.
  str = encodeURIComponent(str);
  // encodeURIComponent does not encode ., !, ~, *, ', (, and ). So we encode
  // these ourselves.
  str = str.replace(/[.!~*'()]/g, function(c) {
    switch (c) {
    case ".":
      return "%2E";
    case "!":
      return "%21";
    case "~":
      return "%7E";
    case "*":
      return "%2A";
    case "'":
      return "%27";
    case "(":
      return "%28";
    case ")":
      return "%29";
    default:
      return c;
    }
  });
  // Finally, we replace % with ~.
  return str.replace(/%/g, "~");
}
