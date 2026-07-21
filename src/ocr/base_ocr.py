"""Base class for PaddleOCR model wrappers."""

from __future__ import annotations

import sys
from abc import ABC, abstractmethod
from typing import Any

from cv2 import typing

# Force UTF-8 console output on Windows to prevent encoding errors
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")


class BaseOCREngine(ABC):
    """Abstract base class for PaddleOCR engines."""

    def __init__(
        self,
        enable_mkldnn: bool = False,
        **kwargs: Any
    ) -> None:
        """
        Initialize the base OCR engine.

        Args:
            enable_mkldnn: If True, enables oneDNN/MKL-DNN acceleration.
                Defaults to False to prevent driver/environment crashes on standard CPUs.
            **kwargs: Additional configuration parameters passed to the underlying model.
        """
        self.enable_mkldnn = enable_mkldnn
        self.kwargs = kwargs
        self._model: Any = None

    @property
    def model(self) -> Any:
        """Lazy loader for the underlying PaddleOCR model instance."""
        if self._model is None:
            self._model = self._initialize_model()
        return self._model

    @abstractmethod
    def _initialize_model(self) -> Any:
        """Initialize the specific PaddleOCR model instance."""
        pass

    @abstractmethod
    def process(
        self,
        img_data: typing.MatLike,
        save_path: str | None = None,
        log_id: str | None = None,
        **kwargs: Any
    ) -> Any:
        """
        Process the image with the loaded model and extract structured results.

        Args:
            img_data: Image data stored as a numpy array.
            save_path: Optional output path to save structured JSON results.
            log_id: Optional log ID for logging.
            **kwargs: Additional keyword arguments passed directly to the model's prediction method.

        Returns:
            Structured results according to the selected model type.
        """
        if log_id:
            print(f"[{log_id}] Processing image for text recognition")
        if not isinstance(img_data, typing.MatLike):
            raise TypeError("img_data must be a numpy array")
