"""Text OCR model implementation using standard PaddleOCR."""
from __future__ import annotations

import json
import os
from typing import Any

import cv2

from ocr.base_ocr import BaseOCREngine


class TextOCREngine(BaseOCREngine):
    """PaddleOCR engine wrapper for standard text recognition (OCR)."""

    def __init__(
        self,
        lang: str = "hu",
        enable_mkldnn: bool = False,
        **kwargs: Any
    ) -> None:
        """
        Initialize the Text OCR engine.

        Args:
            lang: Language code for standard text OCR (e.g., 'hu' for Hungarian).
            enable_mkldnn: If True, enables oneDNN/MKL-DNN acceleration.
                Defaults to False to prevent driver/environment crashes on standard CPUs.
            **kwargs: Additional configuration parameters passed to standard PaddleOCR.
        """
        self.lang = lang
        super().__init__(enable_mkldnn=enable_mkldnn, **kwargs)

    def _initialize_model(self) -> Any:
        """Initialize standard PaddleOCR model."""
        from paddleocr import PaddleOCR
        return PaddleOCR(
            use_textline_orientation=True,
            lang=self.lang,
            enable_mkldnn=self.enable_mkldnn,
            **self.kwargs
        )

    def process(
        self,
        img_data: cv2.typing.MatLike,
        save_path: str | None = None,
        log_id: str | None = None,
        **kwargs: Any
    ) -> list[dict[str, Any]]:
        """
        Process the image to detect and transcribe text with bounding box coordinates.

        Args:
            img_data: Image data stored as a numpy array.
            save_path: Optional output path to save structured JSON results.
            log_id: Optional log ID for logging.
            **kwargs: Extra execution options for predictions.

        Returns:
            A list of dictionaries containing recognized text, confidence scores,
            and bounding box center coordinates (x, y).
        """
        super().process(img_data, log_id=log_id)
        result = self.model.predict(img_data)
        extracted_data: list[dict[str, Any]] = []

        for res in result:
            if res is None:
                continue
            texts = res.get("rec_texts", [])
            scores = res.get("rec_scores", [])
            polys = res.get("dt_polys", [])

            for text, score, poly in zip(texts, scores, polys, strict=False):
                text = text.strip()
                if not text:
                    continue

                if poly.any():
                    y_center = sum(float(p[1]) for p in poly) / len(poly)
                    x_center = sum(float(p[0]) for p in poly) / len(poly)
                else:
                    y_center = 0.0
                    x_center = 0.0

                extracted_data.append({
                    "text": text,
                    "score": float(score),
                    "confidence": float(score),
                    "y": y_center,
                    "x": x_center,
                })

            # Save PaddleOCR internal json if save_path is provided
            if save_path:
                res.save_to_json(save_path=save_path)

        if save_path and not os.path.exists(save_path):
            with open(save_path, "w", encoding="utf-8") as f:
                json.dump(extracted_data, f, ensure_ascii=False, indent=4)

        return extracted_data
