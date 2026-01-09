#!/usr/bin/env python3

import argparse
import sys

from colors import BACKGROUND, WARNING
from database import pg_query_runner, QUERIES

# Library imports with user-friendly error messages.
try:
    from PIL import Image, ImageDraw, ImageFont
except ImportError as e:
    print(f"Error: A required library is not installed. Please install it using 'pip install Pillow'. Missing: {e.name}", file=sys.stderr)
    sys.exit(1)


# Display and font configuration.
FONT_PATH_BOLD = "DejaVuSansMono-Bold.ttf"
FONT_PATH_REGULAR = "DejaVuSansMono.ttf"
IMAGE_WIDTH = 800
IMAGE_HEIGHT = 340
BACKGROUND_COLOR = BACKGROUND
HEADER_COLOR = WARNING
TEXT_COLOR = WARNING

def format_timedelta_to_hhmm(td):
    """Formats a timedelta object into a zero-padded HH:MM string."""
    if not td:
        return "--:--"
    total_seconds = td.total_seconds()
    hours = int(total_seconds // 3600)
    minutes = int((total_seconds % 3600) // 60)
    return f"{hours:02d}:{minutes:02d}"

def draw_frame(departures_to_show, num_rows, header_font, body_font):
    """
    Draws a single frame of the departure board.
    """
    # Create the base image (the board)
    img = Image.new('RGB', (IMAGE_WIDTH, IMAGE_HEIGHT), color=BACKGROUND_COLOR)
    draw = ImageDraw.Draw(img)

    # --- Draw Header ---
    header_y = 10
    draw.text((20, header_y), "ROUTE", font=header_font, fill=HEADER_COLOR)
    draw.text((150, header_y), "DESTINATION", font=header_font, fill=HEADER_COLOR)
    draw.text((680, header_y), "TIME", font=header_font, fill=HEADER_COLOR)
    draw.line([(10, header_y + 30), (IMAGE_WIDTH - 10, header_y + 30)], fill=HEADER_COLOR, width=2)

    # --- Draw Departure Rows ---
    row_height = 35
    start_y = header_y + 45

    for i in range(num_rows):
        y_pos = start_y + (i * row_height)
        if i < len(departures_to_show):
            dep = departures_to_show[i]
            route = dep.get('route', '---')
            destination = dep.get('destination', '---')
            time_str = format_timedelta_to_hhmm(dep.get('time'))

            draw.text((20, y_pos), str(route), font=body_font, fill=TEXT_COLOR)
            draw.text((150, y_pos), str(destination), font=body_font, fill=TEXT_COLOR)
            draw.text((680, y_pos), time_str, font=body_font, fill=TEXT_COLOR)

    return img

def main():
    """
    Main function to parse arguments, run queries, and generate the animation.
    """
    parser = argparse.ArgumentParser(description="Generates an animated departure sign for a given bus stop.")
    parser.add_argument("--stop-id", required=True, help="The stop_id for the departure board.")
    parser.add_argument("--date", required=True, help="Date for the query in YYYY-MM-DD format.")
    parser.add_argument("--time", required=True, help="Start time for the query in HH:MM:SS format.")
    parser.add_argument("--output", required=True, help="Path to save the animated PNG file (e.g., 'departures.png').")
    parser.add_argument("--rows", type=int, default=8, help="Number of departure rows to display on the board.")
    parser.add_argument("--fps", type=int, default=2, help="Frames per second for the animation. A lower number is slower.")
    args = parser.parse_args()

    # Load fonts.
    try:
        header_font = ImageFont.truetype(FONT_PATH_BOLD, 20)
        body_font = ImageFont.truetype(FONT_PATH_REGULAR, 24)
    except IOError:
        print(f"Error: Font files not found. Please check FONT_PATH variables.", file=sys.stderr)
        sys.exit(1)

    # Fetch data.
    all_departures = []
    with pg_query_runner() as runner:
        all_departures = runner(
            QUERIES['postgres']['next_departures'],
            (args.stop_id, args.date, args.time)
        )

    if not all_departures:
        print("No departures found for the given stop, date, and time. Cannot generate animation.", file=sys.stderr)
        return

    # Generate frames.
    frames = []
    # The loop will run once for each departure that can appear at the top of the board.
    # It stops when there are not enough remaining departures to fill the board.
    num_frames = min(120, len(all_departures) - args.rows + 1)
    if num_frames <= 0:
        print("Not enough departures to generate a scrolling animation.", file=sys.stderr)
        # Create a single static frame.
        frame = draw_frame(all_departures, args.rows, header_font, body_font)
        frames.append(frame)
    else:
        print(f"Generating {num_frames} frames (one for each departing trip)...")
        for i in range(num_frames):
            # The list of departures for this frame is a "slice" of the full list.
            # For each iteration, the slice starts one position further down.
            departures_for_frame = all_departures[i : i + args.rows]

            # Draw the image for the current state of the board
            frame = draw_frame(departures_for_frame, args.rows, header_font, body_font)
            frames.append(frame)
            print(f"  - Frame {i+1}/{num_frames} complete.")

    # Save animation.
    if not frames:
        print("No frames were generated.", file=sys.stderr)
        return

    print(f"\nSaving animation to {args.output}...")
    frames[0].save(
        args.output,
        save_all=True,
        append_images=frames[1:],
        duration=1000 // args.fps,  # Duration per frame in milliseconds
        loop=0
    )
    print("Animation saved successfully!")

if __name__ == "__main__":
    main()
