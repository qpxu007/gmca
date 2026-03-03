import sys

from PyQt5.QtCore import Qt, QRect
from PyQt5.QtGui import QIcon, QPixmap, QPainter, QColor, QFont, QPen
from PyQt5.QtWidgets import QApplication, QMainWindow


def generate_icon_with_text(text="A", size=64, bg_color="#3498db", text_color="white"):
    """
    Generates a QIcon dynamically with a background color and text.

    Args:
        text (str): The character(s) to draw on the icon.
        size (int): The width and height of the icon in pixels.
        bg_color (str or QColor): The background color.
        text_color (str or QColor): The text color.

    Returns:
        QIcon: The dynamically generated icon.
    """
    # 1. Create a QPixmap to act as the canvas for our icon
    pixmap = QPixmap(size, size)
    pixmap.fill(Qt.transparent)  # Start with a transparent background

    # 2. Create a QPainter to draw on the pixmap
    painter = QPainter(pixmap)
    painter.setRenderHint(QPainter.Antialiasing)  # Make it look smooth

    # 3. Define colors and fonts
    background_color = QColor(bg_color)
    text_color = QColor(text_color)

    # Set the font. Adjust the point size based on the icon size.
    font_size = int(size * 0.6)  # 60% of the icon size
    font = QFont("Arial", font_size, QFont.Bold)
    painter.setFont(font)

    # 4. Draw the background shape
    #    We'll draw a rounded rectangle.
    painter.setBrush(background_color)
    painter.setPen(Qt.NoPen)  # No outline for the background
    painter.drawRoundedRect(0, 0, size, size, 10, 10)  # x, y, w, h, x-radius, y-radius

    # 5. Draw the text
    painter.setPen(QPen(text_color))
    # Define the rectangle where the text will be drawn to center it
    text_rect = QRect(0, 0, size, size)
    # The flags Qt.AlignCenter centers the text both horizontally and vertically
    painter.drawText(text_rect, Qt.AlignCenter, text)

    # 6. End the painting process
    painter.end()

    # 7. Create a QIcon from the finished QPixmap
    return QIcon(pixmap)


class MyWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("App with Dynamically Generated Icon")
        self.setGeometry(300, 300, 400, 200)

        # --- Generate and set the icon ---
        # You can customize the text, size, and colors here
        app_icon = generate_icon_with_text(text="Q", bg_color="#e74c3c", size=128)
        self.setWindowIcon(app_icon)


if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = MyWindow()
    window.show()
    sys.exit(app.exec_())
