import os
import csv
import shutil
from datetime import datetime
from dotenv import load_dotenv
from loguru import logger

from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.audio.vad.vad_analyzer import VADParams
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import (
    LLMContextAggregatorPair,
    LLMUserAggregatorParams,
)
from pipecat.runner.types import RunnerArguments
from pipecat.runner.utils import parse_telephony_websocket
from pipecat.serializers.exotel import ExotelFrameSerializer
from pipecat.services.cartesia.tts import CartesiaTTSService
from pipecat.services.deepgram.stt import DeepgramSTTService
from pipecat.services.openai.llm import OpenAILLMService
from pipecat.transports.base_transport import BaseTransport
from pipecat.frames.frames import EndFrame, TextFrame
from pipecat.transports.websocket.fastapi import (
    FastAPIWebsocketParams,
    FastAPIWebsocketTransport,
)

load_dotenv(override=True)

# Shared store for booking information, keyed by Exotel call SID
booking_info_store = {}

# ---------------------------------------------------------------------
# CSV helper – update the status of a call in calls.csv
# ---------------------------------------------------------------------
async def update_csv_status(call_id, status):
    if not call_id:
        return
    csv_file = "calls.csv"
    try:
        rows = []
        with open(csv_file, mode="r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            for row in reader:
                if str(row["id"]) == str(call_id):
                    row["status"] = status
                    row["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                rows.append(row)
        with open("calls_tmp.csv", mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        shutil.move("calls_tmp.csv", csv_file)
    except Exception as e:
        logger.error(f"CSV Update Error: {e}")

# ---------------------------------------------------------------------
# Helper to fetch a pending or active call from the CSV (used by the API)
# ---------------------------------------------------------------------
async def get_info(phone):
    csv_file = "calls.csv"
    if not os.path.exists(csv_file):
        return None
    def norm(p):
        return "".join(filter(str.isdigit, str(p)))[-10:]
    target = norm(phone) if phone else None
    with open(csv_file, mode="r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if target:
        for r in rows:
            if norm(r.get("phone_number", "")) == target:
                return r
    active = [r for r in rows if r.get("status", "").lower() in ["in_progress", "processing", "pending"]]
    return active[0] if active else None

# ---------------------------------------------------------------------
# Core bot logic – builds the Pipecat pipeline and runs it
# ---------------------------------------------------------------------
async def run_bot(transport, handle_sigint, call_sid, info):
    # Resolve date / time fields (fallback to generic values)
    date = info.get("booking_date") or info.get("date") or "today"
    time = info.get("booking_time") or info.get("time") or "now"
    cid = info.get("id") or info.get("call_id")

    llm = OpenAILLMService(
        api_key=os.getenv("OPENAI_API_KEY"),
        settings=OpenAILLMService.Settings(model="gpt-4o-mini"),
    )
    stt = DeepgramSTTService(api_key=os.getenv("DEEPGRAM_API_KEY"))
    tts = CartesiaTTSService(
        api_key=os.getenv("CARTESIA_API_KEY"),
        settings=CartesiaTTSService.Settings(
            voice="7ea5e9c2-b719-4dc3-b870-5ba5f14d31d8"
        ),
    )

    greeting = f"Hi, I'm calling from Humming Bird to confirm if a room is available on {date} at {time}. Is the room available at that date and time?"
    context = LLMContext(
        messages=[
            {
                "role": "system",
                "content": (
                    f"You are an agent calling on behalf of Humming Bird. "
                    f"When the hotel staff answers and says anything (like 'Hello'), "
                    f"your FIRST and ONLY response must be exactly: \"{greeting}\" "
                    f"After they reply, say 'Thank you, Goodbye.' and nothing else. "
                    f"Never ask follow-up questions. Keep every response very short."
                ),
            }
        ]
    )
    user_aggregator, assistant_aggregator = LLMContextAggregatorPair(
        context,
        user_params=LLMUserAggregatorParams(vad_analyzer=SileroVADAnalyzer(params=VADParams(stop_secs=0.6))),
    )
    pipeline = Pipeline([
        transport.input(),
        stt,
        user_aggregator,
        llm,
        tts,
        transport.output(),
        assistant_aggregator,
    ])
    task = PipelineTask(
        pipeline,
        params=PipelineParams(
            audio_in_sample_rate=8000,
            audio_out_sample_rate=8000,
            enable_metrics=True,
            enable_usage_metrics=True,
        ),
    )

    async def end_call(function_name, tool_call_id, args, llm, context, result_callback):
        logger.info("AI triggered end_call! Hanging up...")
        await result_callback("Call successfully ended.")
        await task.queue_frame(EndFrame())

    llm.register_function("end_call", end_call)

    @transport.event_handler("on_client_connected")
    async def on_client_connected(transport, client):
        logger.info("Call connected – waiting for hotel to speak before responding")

    @transport.event_handler("on_client_disconnected")
    async def on_client_disconnected(transport, client):
        await task.cancel()

    runner = PipelineRunner(handle_sigint=handle_sigint)
    await runner.run(task)

async def bot(args: RunnerArguments):
    try:
        _, data = await parse_telephony_websocket(args.websocket)
        sid = data["call_id"]
        info = booking_info_store.get(sid) or await get_info(data.get("to")) or await get_info(data.get("from"))
        
        transport = FastAPIWebsocketTransport(
            websocket=args.websocket,
            params=FastAPIWebsocketParams(
                audio_in_enabled=True,
                audio_out_enabled=True,
                add_wav_header=False,
                serializer=ExotelFrameSerializer(stream_sid=data["stream_id"], call_sid=sid),
            ),
        )
        await run_bot(transport, args.handle_sigint, sid, info or {})
    except Exception as e:
        logger.error(f"Bot Error: {e}")