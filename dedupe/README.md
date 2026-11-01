# Dedupe Training

Run commands from the repository root after installing the dedupe extra:

```bash
poetry install -E dedupe
```

On Windows, the native build prerequisites in the root README also apply.

## Local Files

- `dedupe/training_events.json`: the event corpus used for training and selecting known pairs.
- `dedupe/training_data.json`: accumulated labels saved by Dedupe, containing `match` and `distinct` pairs of complete records. This is the single label file to keep and reuse between sessions.
- `dedupe/settings.dedupe`: the trained model used when processing event lists.

Event corpora and training labels are private local files, ignored by Git. They are not uploaded to the public repository. Keep private backups before training or replacing them. The saved model
does not replace the labeled examples needed for retraining.

## Event Input

The event corpus is a JSON list of wrapped events. Each wrapper associates a source with one event; `summary` and `dtstart` are required. For example:

```json
[
  {
    "source_a": {
      "summary": "Example exhibition",
      "dtstart": "2030-07-14T10:00:00+02:00",
      "location": "Example venue"
    }
  },
  {
    "source_b": {
      "summary": "Example exhibition listing",
      "dtstart": "2030-07-14",
      "location": "Example venue"
    }
  }
]
```

Provide a representative corpus with duplicates and unrelated events, not just these illustrative records. The normalizer prepares comparison fields automatically, including text normalization and Budapest-local dates. Do not manually add normalized fields to the input corpus.

## Training Process

Every training run loads any saved labels from `dedupe/training_data.json`, optionally adds labels through the pairing steps, and then trains a fresh model:

1. **Load saved labels.** Existing `match` and `distinct` pairs are loaded and their normalized fields are refreshed. If the label file does not exist, training starts without saved labels.
2. **Manual pairing.** You select specific pairs by event ID (see below). Decisions are saved to the label file when this step ends.
3. **Automatic pairing.** Dedupe's console proposes pairs for you to label as matching or distinct. Finish the session to continue.
4. **Train.** The model is trained from all saved labels. The label file and `dedupe/settings.dedupe` are then written. Labels accumulate across sessions because the same label file is read and written each time.

The pairing steps can be combined in four ways:

- **Manual, then automatic (default):** adds labels by hand, then by console labeling.

  ```bash
  poetry run event-clusterizer train
  ```

- **Automatic only (`--no-manual-pairing`):** skips step 2 and labels only the pairs proposed by Dedupe.

  ```bash
  poetry run event-clusterizer train --no-manual-pairing
  ```

- **Manual only (`--no-auto-pairing`):** lets you select known pairs, then trains from the existing and manually added labels without Dedupe proposing more pairs.

  ```bash
  poetry run event-clusterizer train --no-auto-pairing
  ```

- **Saved labels only (both switches):** skips both pairing steps and trains without prompts. Use this after changing normalization logic or adding comparison fields: saved records are reprocessed with the current normalizer, and a fresh model is trained with the current field configuration. The refreshed labels and new settings are saved; previous model weights are not reused.

  ```bash
  poetry run event-clusterizer train --no-manual-pairing --no-auto-pairing
  ```

Whenever automatic pairing is disabled, both `match` and `distinct` labels must already be available after the optional manual step, with enough examples for training. New comparison fields must be derivable from the saved records; otherwise enrich the records or collect new examples before retraining.

Run `poetry run event-clusterizer train --help` for additional other options.

### Manual Pairing Commands

- `list`: show input event IDs, titles, dates, locations and sources.
- `list title text`: show events whose titles contain the given text, ignoring letter case.
- `match ID ID`: label two different events as the same event.
- `distinct ID ID`: label two different events as different events.
- `done`: finish manual pairing.

IDs are zero-based positions in the concatenated input corpus and can change when the input changes, so use the IDs shown by `list` in the current session. Saved labels store complete records rather than IDs. Re-labeling a pair replaces its previous decision.

## Training Notes

Changing normalization or comparison fields requires retraining the saved settings. Dedupe learns from labeled comparisons; shared dates or towns alone do not prove that records describe the same event. Training examples are not an independent accuracy test. Keep separately reviewed examples to assess behavior on unseen events.
