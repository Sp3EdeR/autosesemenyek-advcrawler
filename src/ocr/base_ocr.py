"""Base class for PaddleOCR model wrappers."""

from __future__ import annotations

import os
import sys
from abc import ABC, abstractmethod
from typing import Any, Optional

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
        img_path: str,
        save_path: Optional[str] = None,
        **kwargs: Any
    ) -> Any:
        """
        Process the image with the loaded model and extract structured results.

        Args:
            img_path: Absolute or relative path to the target image.
            save_path: Optional output path to save structured JSON results.
            **kwargs: Additional keyword arguments passed directly to the model's prediction method.

        Returns:
            Structured results according to the selected model type.
        """
        if not os.path.exists(img_path):
            raise FileNotFoundError(f"Target image not found at path: {img_path}")
