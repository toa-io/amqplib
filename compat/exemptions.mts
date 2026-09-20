// What of the original's suite is not run, and why. Everything else in `compat/test` is.

/** Whole files. */
export const files: Record<string, string> = {
  'mux.test.js':
    'Tests `lib/mux`, the stream multiplexer that fed channel buffers to the socket in turns. ' +
    'There is no such module here: frames are encoded where they go out from, and nothing ' +
    'observable depended on the multiplexer.',
}

/** Single tests, by name. */
export const tests: Record<string, string> = {
  'throw in close handler kills the connection without handler-error listener':
    'Expects an uncaught ERR_STREAM_PUSH_AFTER_EOF: the original writes the CloseOk it owes ' +
    'after it has ended the stream, and the test waits for that write to fail. Here it is ' +
    'written before the stream is ended. The rest of what the test asserts is covered by ' +
    '`src/connection.test.ts`.',
}
