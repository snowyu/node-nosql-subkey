util        = require("abstract-object/lib/util")
inherits    = util.inherits
Errors      = require("abstract-object/Error")
createError = Errors.createError

Errors.SubkeyError   = SubkeyError = createError('Subkey', 299)
Errors.RedirectError = RedirectError = createError("Redirect", 300, SubkeyError)
Errors.RedirectExceedError = createError("RedirectExceed", 301, RedirectError)
Errors.LoadingError  = createError("Loading", 302, SubkeyError)

module.exports = Errors

