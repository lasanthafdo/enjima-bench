### AI Slop from Gemini ###

import matplotlib as mpl
import numpy as np

# Get a colormap object. You can replace 'viridis' with any other valid colormap name.
# The second argument (10) specifies the number of discrete colors to sample.
n_colors = 20
cmap = mpl.colormaps['tab20'].resampled(n_colors)

# Sample the colormap to get a list of RGBA tuples
colors_rgba = cmap(np.linspace(0, 1, n_colors))

# Convert each RGBA tuple to a hex string
hex_colors = [mpl.colors.to_hex(color) for color in colors_rgba]

# Print the list of hex codes
print(hex_colors)