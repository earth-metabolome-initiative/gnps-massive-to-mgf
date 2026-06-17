# Recording the TUI demo GIF

The README embeds `docs/tui-demo.gif`. Regenerate it with `asciinema` + `agg` + `gifsicle`. All three tools are expected to be on `$PATH`. If `gifsicle` is missing, install it with `sudo apt install gifsicle`.

## Workflow

1. Build the release binary (the recording will run the real downloader against MassIVE, so the binary must already exist):

   ```bash
   cargo build --release
   ```

2. Resize your terminal to roughly 160x40 (or whatever you want the GIF to be). `agg` honors the recorded geometry, so the cast dimensions decide the final GIF dimensions.

3. Record the session. `--overwrite` lets you re-record into the same path. `--idle-time-limit 1` caps any single pause to 1 s of wall-clock time so a real FTPS stall does not balloon the recording:

   ```bash
   asciinema rec docs/tui-demo.cast \
     --overwrite \
     --idle-time-limit 1
   ```

4. Inside the recording shell, launch the TUI:

   ```bash
   ./target/release/massive download
   ```

   Drive it for 30 to 60 seconds. Switch through every tab (left click, or `Tab` / `Shift+Tab`), scroll the log with the mouse wheel, optionally press `d` to demonstrate the dump-to-file popup. End with `q` to exit the TUI, then `Ctrl+D` (or `exit`) at the shell prompt to end the recording.

5. Convert the cast to a GIF. The flags below cap idle frames, mildly speed up playback, and pin the font to one `agg` ships with so the output is reproducible on any machine that has the same `agg` version:

   ```bash
   agg docs/tui-demo.cast docs/tui-demo.gif \
     --theme monokai \
     --font-size 14 \
     --speed 1.5 \
     --idle-time-limit 1
   ```

6. Optimize the GIF for size. `gifsicle -O3` does lossless restructuring, `--lossy=80` shaves another factor of 2 to 3 with minor visual cost:

   ```bash
   gifsicle -O3 --lossy=80 docs/tui-demo.gif -o docs/tui-demo.gif
   ```
