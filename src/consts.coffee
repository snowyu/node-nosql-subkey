consts =
  READ_OP     : 0x40
  WRITE_OP    : 0x80
  GET_OP      : 0x41  # READ_OP | 1
  LIST_OP     : 0x42  # READ_OP | 2
  ADD_OP      : 0x81  # WRITE_OP | 1
  UPDATE_OP   : 0x82  # WRITE_OP | 2
  PUT_OP      : 0x83  # WRITE_OP | ADD_OP | UPDATE_OP
  DEL_OP      : 0x84
  TRANS_OP    : 0x87  # ADD_OP | UPDATE_OP | DEL_OP
  HALT_OP     : -1
  CONTINUE_OP : 0
  SKIP_OP     : 1

module.exports = consts

