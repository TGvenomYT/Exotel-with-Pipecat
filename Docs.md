# 🗂️ Hotel Calling Bot System Specification (SYSTEM_SPECIFICATION.md)

## 🌟 Overview
This system automates the process of calling hotels to confirm room availability. It functions as a multi-stage, fault-tolerant, voice-based automation pipeline.

**Primary Goal:** Check room availability for a scheduled date/time and log the full conversation transcript into a persistent JSON record.

## 🔗 System Architecture (Conceptual Flow)
The process moves sequentially through four main components:
1.  **Scheduler/Worker (`automate_calls.py`):** The master brain. It reads the task list from the CSV, marks jobs as `in_progress`, triggers the API call, and marks the job as `completed` or `failed`.
2.  **API Service (`server.py`):** The execution endpoint. It receives the call request via HTTP POST, establishes the WebSocket connection, and runs the multi-stage pipeline.
3.  **External Services:** Exotel (Call Control), Deepgram (STT), OpenAI (LLM), Cartesia (TTS).
4.  **Persistence:** All state and transcripts are saved to the file system (`calls.csv`, `response.json`).

---

## 🧱 Component Deep Dive

### 1. `automate_calls.py` (Scheduler/Worker)
*   **Role:** Manages the work queue from `calls.csv`.
*   **Key Feature:** Uses `asyncio.Lock` and `@retry` decorators to ensure that multiple workers running concurrently do not overwrite each other's progress, making the scheduler highly fault-tolerant.
*   **State Management:** It is the sole authority on updating the `status` and `updated_at` timestamps in `calls.csv`.

### 📝 2. `server.py` (API/Bot Handler)
*   **Role:** Manages the real-time, voice-based conversation.
*   **State Management (FIXED):** It no longer uses an in-memory store. It must now read the booking context (`date`, `time`, etc.) by querying `calls.csv` using the `call_sid` provided during the WebSocket connection.
*   **Core Logic:** The `run_bot` function orchestrates the pipeline:
    *   **Connection:** Uses the `call_sid` to find the booking context in the CSV.
    *   **Call:** Sends the initial greeting via TTS.
    *   **Conversation:** Waits for STT $\rightarrow$ LLM $\rightarrow$ TTS loop to process the conversation.
    *   **Exit:** Saves the transcript and signals success by calling `update_call_status`.

### 💾 3. Data Structure Mapping
| Data Point | Storage Location | Purpose |
| :--- | :--- | :--- |
| **Job Manifest** | `calls.csv` | Master list of all jobs, their status, and the core booking parameters. |
| **Call Transcript** | `response.json` | A historical record of the *actual conversation* that occurred. |
| **Conversation Context** | (Should be read from `calls.csv`) | The initial parameters (`date`, `time`) the bot must know to frame the conversation. |

### 🚧 4. Major Outstanding Issue: The Handshake Problem (UX)
This is the most critical user experience bug. The bot's current behavior is either **speaking too soon** or **speaking too late**.

**Required Fix:** The bot must implement a **"Quiet Acknowledgment Handshake"**.
*   **The Goal:** Do nothing audible immediately upon connection. Wait for the first sound from the human. *Only after detecting speech* should the bot pause for a moment, and *then* deliver the scripted greeting.

---
***End of Document***