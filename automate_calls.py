
#Excel
'''import asyncio
import aiohttp
import csv
import json
import os
import shutil
from datetime import datetime
from dotenv import load_dotenv
from loguru import logger

load_dotenv(override=True)

# CSV configuration
CSV_FILE = 'calls.csv'
CSV_TEMP_FILE = 'calls_temp.csv'

# Hotel agent API endpoint
AGENT_URL = os.getenv('AGENT_URL', 'http://localhost:7862/start')

# Number of concurrent calls (2 channels)
MAX_CONCURRENT_CALLS = int(os.getenv('MAX_CONCURRENT_CALLS', '2'))

class CallProcessor:
    def __init__(self):
        self.processing = True
        self.lock = asyncio.Lock()

    async def get_all_calls(self):
        """Read all calls from CSV file"""
        calls = []
        try:
            if not os.path.exists(CSV_FILE):
                logger.error(f"{CSV_FILE} does not exist")
                return []
                
            with open(CSV_FILE, mode='r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    calls.append(row)
            return calls
        except Exception as e:
            logger.error(f"Error reading CSV: {e}")
            return []

    async def save_all_calls(self, calls):
        """Save all calls back to CSV file"""
        try:
            if not calls:
                return
                
            fieldnames = list(calls[0].keys())
            with open(CSV_TEMP_FILE, mode='w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(calls)
            
            # Atomic swap
            shutil.move(CSV_TEMP_FILE, CSV_FILE)
        except Exception as e:
            logger.error(f"Error saving CSV: {e}")

    async def claim_next_pending_call(self):
        """Claim the next pending call from CSV."""
        async with self.lock:
            try:
                calls = await self.get_all_calls()
                if not calls:
                    logger.warning("No calls found in CSV file")
                    return None
                    
                pending_count = 0
                for row in calls:
                    status = row.get('status', '').strip().lower()
                    if status in ['', 'pending', 'failed']:
                        pending_count += 1
                
                logger.info(f"Found {pending_count} pending or retryable calls in CSV")
                
                for row in calls:
                    status = row.get('status', '').strip().lower()
                    if status in ['', 'pending', 'failed']:
                        row['status'] = 'in_progress'
                        row['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        await self.save_all_calls(calls)
                        
                        call_data = {
                            'id': row['id'],
                            'phone_number': row['phone_number'].strip(),
                            'booking_date': row['booking_date'].strip(),
                            'booking_time': row['booking_time'].strip(),
                            'day_of_week': row['day_of_week'].strip()
                        }
                        logger.info(f"Successfully claimed call ID {call_data['id']} for {call_data['phone_number']}")
                        return call_data
                return None
            except Exception as e:
                logger.error(f"Error claiming pending call: {e}")
                return None

    async def format_date_for_bot(self, date_string):
        """Format date string for better readability in bot conversation"""
        try:
            date_obj = datetime.strptime(date_string, '%Y-%m-%d')
            return date_obj.strftime('%B %d, %Y')  # e.g., "December 25, 2024"
        except Exception as e:
            logger.error(f"Error formatting date {date_string}: {e}")
            return date_string

    async def make_call_with_data(self, phone_number, booking_date, booking_time, day_of_week, call_id):
        """Make a call using the hotel agent API, passing booking info in the payload."""
        try:
            # Format the date for the bot
            formatted_date = await self.format_date_for_bot(booking_date)
            
            async with aiohttp.ClientSession() as session:
                payload = {
                    "dialout_settings": {
                        "phone_number": phone_number
                    },
                    "booking_info": {
                        "date": formatted_date,
                        "time": booking_time,
                        "day_of_week": day_of_week,
                        "call_id": call_id
                    }
                }
                
                async with session.post(AGENT_URL, json=payload) as response:
                    if response.status == 200:
                        result = await response.json()
                        logger.info(f"Call initiated successfully for {phone_number}: {result}")
                        return result
                    else:
                        error_text = await response.text()
                        logger.error(f"Failed to initiate call for {phone_number}: {error_text}")
                        return None
        except Exception as e:
            logger.error(f"Error making call to {phone_number}: {e}")
            return None

    async def update_call_status(self, call_id, status):
        """Update call status in CSV"""
        async with self.lock:
            try:
                calls = await self.get_all_calls()
                updated = False
                for row in calls:
                    if row['id'] == str(call_id):
                        row['status'] = status
                        row['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        updated = True
                        break
                
                if updated:
                    await self.save_all_calls(calls)
                    logger.info(f"Updated call {call_id} status to {status}")
                else:
                    logger.warning(f"Call {call_id} not found for status update")
                    
            except Exception as e:
                logger.error(f"Error updating call status for {call_id}: {e}")

    async def process_single_call(self, call_data):
        """Process a single call completely"""
        try:
            logger.info(f"Processing call {call_data['id']} for {call_data['phone_number']}")
            
            # Make the call
            result = await self.make_call_with_data(
                call_data['phone_number'],
                call_data['booking_date'],
                call_data['booking_time'],
                call_data['day_of_week'],
                call_data['id']
            )
            
            if result:
                # Update status to 'processing' (bot will complete the call)
                await self.update_call_status(call_data['id'], 'processing')
                logger.info(f"Successfully initiated call {call_data['id']}")
            else:
                # Update status to 'failed'
                await self.update_call_status(call_data['id'], 'failed')
                logger.error(f"Failed to initiate call {call_data['id']}")
                
            return result
            
        except Exception as e:
            logger.error(f"Error processing call {call_data['id']}: {e}")
            await self.update_call_status(call_data['id'], 'error')
            return None

    async def call_worker(self, worker_id):
        """Worker function to process calls continuously"""
        logger.info(f"Call worker {worker_id} started")
        
        while self.processing:
            try:
                # Atomically claim the next pending call to ensure exclusive processing
                call_data = await self.claim_next_pending_call()
                
                if call_data:
                    # Process this call completely
                    await self.process_single_call(call_data)
                    
                    # Small delay to prevent overwhelming the system
                    await asyncio.sleep(2)
                else:
                    # No pending calls, wait before checking again
                    logger.info(f"Worker {worker_id}: No pending calls found, waiting...")
                    await asyncio.sleep(30)
                    
            except Exception as e:
                logger.error(f"Worker {worker_id}: Error in main loop: {e}")
                await asyncio.sleep(10)
        
        logger.info(f"Call worker {worker_id} stopped")

    async def start_processing(self):
        """Start processing calls with multiple workers"""
        logger.info(f"Starting call processing with {MAX_CONCURRENT_CALLS} concurrent workers...")
        
        # Create worker tasks
        tasks = []
        for i in range(MAX_CONCURRENT_CALLS):
            task = asyncio.create_task(self.call_worker(i))
            tasks.append(task)
        
        # Wait for all workers (they run indefinitely until stopped)
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"Error in worker management: {e}")

    async def stop_processing(self):
        """Stop processing calls"""
        self.processing = False
        logger.info("Stopping call processing...")

async def main():
    """Main entry point"""
    processor = CallProcessor()
    
    try:
        await processor.start_processing()
    except KeyboardInterrupt:
        logger.info("Automation stopped by user")
        await processor.stop_processing()
    except Exception as e:
        logger.error(f"Error in main: {e}")
        await processor.stop_processing()

if __name__ == "__main__":
    # Run the automation
    asyncio.run(main())'''

#Database
'''import os
import json
import time
from datetime import datetime
from dotenv import load_dotenv
from loguru import logger
import pyodbc

# --- 1. CONFIGURATION & SETUP ---
load_dotenv(override=True)

DB_CONFIG = {
    'server': os.getenv('DB_SERVER'),
    'database': os.getenv('DB_DATABASE'),
    'username': os.getenv('DB_USERNAME'),
    'password': os.getenv('DB_PASSWORD')
}

# --- 2. CORE DATABASE UTILITY ---

def get_db_connection():
    """Establishes and returns a connection to the MSSQL database."""
    if not all([DB_CONFIG['server'], DB_CONFIG['database'], DB_CONFIG['username'], DB_CONFIG['password']]):
        logger.error("Database configuration missing. Please set all required environment variables in .env file.")
        raise EnvironmentError("Database credentials not fully set.")
        
    # Note: Ensure ODBC Driver 17 (or compatible) is installed on the system running this script
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']}"
    )
    try:
        conn = pyodbc.connect(conn_str)
        logger.success("Successfully connected to the SQL Server database.")
        return conn
    except pyodbc.Error as ex:
        sql_error = ex.args[0]
        logger.error(f"Database Connection Error: {sql_error}")
        raise ConnectionError(f"Failed to connect to the database. Check server status and credentials.")


# --- 3. CORE LOGIC FUNCTION: CLAIMING A JOB ---

def claim_next_pending_call_from_db(conn):
    """
    Executes the atomic transaction to claim the next pending call.
    Returns: A dictionary of call data OR None if no call is found.
    """
    logger.info("Attempting to claim the next pending call from the database...")
    
    # The entire block is run as a transaction for atomicity (ACID)
    cursor = conn.cursor()
    
    # The T-SQL logic rewritten into a single execution string for pyodbc.cursor.execute()
    # This mimics the transaction logic: SELECT TOP 1 -> UPDATE -> OUTPUT
    query = """
    BEGIN TRANSACTION;

    -- 1. Select the candidate call to be processed
    DECLARE @NextCallID INT;
    DECLARE @NextCallSid VARCHAR(50);

    SELECT TOP 1 @NextCallID = id, @NextCallSid = CAST(phone_number AS VARCHAR(50)) 
    FROM call_scheduling_table
    WHERE status = 'pending' AND booking_date >= CAST(GETDATE() AS DATE)
    ORDER BY booking_date, booking_time;

    -- 2. IF a call is found, update status AND log the session ID
    IF @NextCallID IS NOT NULL AND @NextCallSid IS NOT NULL
    BEGIN
        UPDATE call_scheduling_table
        SET status = 'in_progress', updated_at = GETDATE()
        WHERE id = @NextCallID;
        
        -- 3. CRITICAL: Initialize the live session tracker
        INSERT INTO live_sessions (session_id, call_id, status, booking_date, booking_time, day_of_week, updated_by)
        VALUES (@NextCallSid, @NextCallID, 'CONNECTING', @BookingDate, @BookingTime, @DayOfWeek, 'WorkerService');
        
        SELECT 'SUCCESS', @NextCallID, @NextCallSid; -- Signal success
    END
    ELSE
    BEGIN
        SELECT 'NO_TASK', NULL, NULL; -- Signal no task found
    END
    
    COMMIT TRANSACTION;
    """
    
    # We need to pass the necessary variables into the query execution context
    cursor.execute(query, 
                   # We pass dummy values for the WHERE/SELECT context, 
                   # as the logic uses variables declared internally.
                   # The values used in the IF block (@BookingDate, etc.) 
                   # will be bound by the cursor library if they are needed in the final SELECT/OUTPUT.
                   # For this complex structure, passing an empty tuple often suffices if the variables are declared in the query itself.
                   (None, None, None, None, None, None) 
                  )
    
    # Fetch the result set from the cursor
    result = cursor.fetchone()
    
    # The result structure matches what we expect from the success/failure signal.
    if result:
        result_type, call_id, call_sid = result
        if result_type == 'SUCCESS':
            logger.info(f"Successfully claimed task. New Call SID: {call_sid}")
            return {
                'id': call_id,
                'phone_number': call_sid,
                'booking_date': result[2], # Assuming column indices based on SELECT TOP 1
                'booking_time': result[3],
                'day_of_week': result[4],
                'status': 'pending'
            }
        elif result_type == 'NO_TASK':
            return None
            
    return None


# --- 4. THE MAIN WORKER LOOP ---

def worker_main_loop():
    """
    The main loop that continuously checks the DB for work.
    This function should be run in a dedicated, persistent process.
    """
    conn = None
    try:
        conn = get_db_connection()
        
        while True:
            logger.info("--- Worker starting cycle. Attempting to claim task. ---")
            
            # 1. Claim Task
            call_data = claim_next_pending_call_from_db(conn)
            
            if call_data:
                logger.success(f"✅ Successfully claimed task for {call_data['phone_number']} (ID: {call_data['id']}). Starting processing sequence...")
                
                # 2. Execute Business Logic (This is where you'd call the WebSockets/APIs)
                # For now, we just simulate success:
                
                # --- PLACEHOLDER FOR THE ACTUAL PROCESSING ---
                # In a real setup, you would spawn the WebSocket handler here:
                # websocket_client.connect(call_data['phone_number']) 
                # and then run the session management logic.
                
                print("--- SIMULATION: Task claimed. In a real system, the WebClient would now be initiated. ---")
                
                # 3. Wait before checking again (Prevents rapid-fire polling)
                time.sleep(15) 
                
            else:
                logger.info("🟡 No pending tasks found in the queue. Sleeping for 30 seconds.")
                time.sleep(30)

    except ConnectionError as e:
        logger.critical(f"CRITICAL FAILURE: {e}. Retrying in 60 seconds...")
        time.sleep(60)
        # Recursive call to keep trying after a long wait
        worker_main_loop()
    except KeyboardInterrupt:
        logger.info("Worker service manually stopped.")
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")


if __name__ == "__main__":
    try:
        # This function must run continuously in a background process/daemon.
        worker_main_loop()
    except Exception as e:
        logger.error(f"Fatal error during worker execution: {e}")'''




import asyncio
import aiohttp
import csv
import os
import shutil
from datetime import datetime
from dotenv import load_dotenv
from loguru import logger
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

# CSV configuration
CSV_FILE = 'calls.csv'
CSV_TEMP_FILE = 'calls_temp.csv'

# Hotel agent API endpoint
AGENT_URL = os.getenv('AGENT_URL', 'http://localhost:7862/start')

# Number of concurrent calls (2 channels)
MAX_CONCURRENT_CALLS = int(os.getenv('MAX_CONCURRENT_CALLS', '2'))

class CallProcessor:
    def __init__(self):
        self.processing = True
        self.lock = asyncio.Lock()

    async def get_all_calls(self):
        """Read all calls from CSV file"""
        calls = []
        try:
            if not os.path.exists(CSV_FILE):
                logger.error(f"{CSV_FILE} does not exist")
                return []
                
            with open(CSV_FILE, mode='r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    calls.append(row)
            logger.info(f"Read {len(calls)} calls from CSV")
            
            return calls
        except Exception as e:
            logger.error(f"Error reading CSV: {e}")
            return []

    async def save_all_calls(self, calls):
        """Save all calls back to CSV file"""
        try:
            if not calls:
                return
                
            fieldnames = list(calls[0].keys())
            with open(CSV_TEMP_FILE, mode='w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(calls)
            
            # Atomic swap
            shutil.move(CSV_TEMP_FILE, CSV_FILE)
        except Exception as e:
            logger.error(f"Error saving CSV: {e}")

    async def claim_next_pending_call(self):
        """Claim the next pending call from CSV."""
        async with self.lock:
            try:
                calls = await self.get_all_calls()
                if not calls:
                    logger.warning("No calls found in CSV file")
                    return None
                    
                pending_count = 0
                for row in calls:
                    status = row.get('status', '').strip().lower()
                    if status in ['', 'pending', 'failed']:
                        pending_count += 1
                
                logger.info(f"Found {pending_count} pending or retryable calls in CSV")
                
                for row in calls:
                    status = row.get('status', '').strip().lower()
                    if status in ['', 'pending', 'failed']:
                        row['status'] = 'in_progress'
                        row['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        await self.save_all_calls(calls)
                        
                        call_data = {
                            'id': row['id'],
                            'phone_number': row['phone_number'].strip(),
                            'booking_date': row['booking_date'].strip(),
                            'booking_time': row['booking_time'].strip(),
                            'day_of_week': row['day_of_week'].strip()
                        }
                        logger.info(f"Successfully claimed call ID {call_data['id']} for {call_data['phone_number']}")
                        return call_data
                return None
            except Exception as e:
                logger.error(f"Error claiming pending call: {e}")
                return None

    async def format_date_for_bot(self, date_string):
        """Format date string for better readability in bot conversation"""
        try:
            date_obj = datetime.strptime(date_string, '%Y-%m-%d')
            return date_obj.strftime('%B %d, %Y')  # e.g., "December 25, 2024"
        except Exception as e:
            logger.error(f"Error formatting date {date_string}: {e}")
            return date_string

    @retry(
        # Retry on network errors or any unexpected exception during the API call
        retry=retry_if_exception_type((aiohttp.ClientError, Exception)),
        # Wait exponentially: 1s, 2s, 4s, 8s... (up to 60s)
        wait=wait_exponential(multiplier=1, min=1, max=60),
        # Stop trying after 5 total attempts (1 initial + 4 retries)
        stop=stop_after_attempt(5),
        reraise=True # Re-raise the exception after all retries fail
    )
    async def make_call_with_data(self, phone_number, booking_date, booking_time, day_of_week, call_id):
        """Make a call using the hotel agent API, passing booking info in the payload. Retry logic added here."""
        try:
            # Format the date for the bot
            formatted_date = await self.format_date_for_bot(booking_date)
            
            async with aiohttp.ClientSession() as session:
                payload = {
                    "dialout_settings": {
                        "phone_number": phone_number
                    },
                    "booking_info": {
                        "date": formatted_date,
                        "time": booking_time,
                        "day_of_week": day_of_week,
                        "call_id": call_id
                    }
                }
                
                # This API call is now protected by @retry above
                async with session.post(AGENT_URL, json=payload) as response:
                    if response.status == 200:
                        result = await response.json()
                        logger.info(f"Call initiated successfully for {phone_number}: {result}")
                        return result
                    else:
                        error_text = await response.text()
                        logger.error(f"Exotel API error ({response.status}): {error_text}")
                        # Raise an exception here to force tenacity to retry
                        raise Exception(f"Exotel API error ({response.status})")
        except Exception as e:
            logger.error(f"Critical error after all retries for {phone_number}: {e}")
            # The final failure must be explicitly raised or handled to stop the process.
            raise e


    async def update_call_status(self, call_id, status):
        """Update call status in CSV"""
        async with self.lock:
            try:
                calls = await self.get_all_calls()
                updated = False
                for row in calls:
                    if row['id'] == str(call_id):
                        row['status'] = status
                        row['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        updated = True
                        break
                
                if updated:
                    await self.save_all_calls(calls)
                    logger.info(f"Updated call {call_id} status to {status}")
                else:
                    logger.warning(f"Call {call_id} not found for status update")
                    
            except Exception as e:
                logger.error(f"Error updating call status for {call_id}: {e}")

    async def process_single_call(self, call_data):
        """Process a single call completely"""
        try:
            logger.info(f"Attempting to process call {call_data['id']} for {call_data['phone_number']}")
            
            # Make the call (this function now contains the retry logic)
            result = await self.make_call_with_data(
                call_data['phone_number'],
                call_data['booking_date'],
                call_data['booking_time'],
                call_data['day_of_week'],
                call_data['id']
            )
            
            if result:
                # Update status to 'processing' (bot will complete the call)
                await self.update_call_status(call_data['id'], 'processing')
                logger.info(f"Successfully initiated call {call_data['id']}")
            else:
                # This path is unlikely to be hit if make_call_with_data raises on failure
                await self.update_call_status(call_data['id'], 'failed')
                logger.error(f"Failed to initiate call {call_data['id']} without a clear error.")
                
            return result
            
        except Exception as e:
            # This catches the final exception after all retries have failed
            logger.error(f"FATAL: Failed to process call {call_data['id']} after all retries. Marking as error: {e}")
            await self.update_call_status(call_data['id'], 'failed_final_retry')
            return None

    async def call_worker(self, worker_id):
        """Worker function to process calls continuously"""
        logger.info(f"Call worker {worker_id} started")
        
        while self.processing:
            try:
                # Atomically claim the next pending call to ensure exclusive processing
                call_data = await self.claim_next_pending_call()
                
                if call_data:
                    # Process this call completely
                    await self.process_single_call(call_data)
                    
                    # Small delay to prevent overwhelming the system
                    await asyncio.sleep(2)
                else:
                    # No pending calls, wait before checking again
                    logger.info(f"Worker {worker_id}: No pending calls found, waiting...")
                    await asyncio.sleep(30)
                    
            except Exception as e:
                logger.error(f"Worker {worker_id}: Error in main loop: {e}")
                await asyncio.sleep(10)
        
        logger.info(f"Call worker {worker_id} stopped")

    async def start_processing(self):
        """Start processing calls with multiple workers"""
        logger.info(f"Starting call processing with {MAX_CONCURRENT_CALLS} concurrent workers...")
        
        # Create worker tasks
        tasks = []
        for i in range(MAX_CONCURRENT_CALLS):
            task = asyncio.create_task(self.call_worker(i))
            tasks.append(task)
        
        # Wait for all workers (they run indefinitely until stopped)
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"Error in worker management: {e}")

    async def stop_processing(self):
        """Stop processing calls"""
        self.processing = False
        logger.info("Stopping call processing...")

async def main():
    """Main entry point"""
    processor = CallProcessor()
    
    try:
        await processor.start_processing()
    except KeyboardInterrupt:
        logger.info("Automation stopped by user")
        await processor.stop_processing()
    except Exception as e:
        logger.error(f"Error in main: {e}")
        await processor.stop_processing()

if __name__ == "__main__":
    # Run the automation
    asyncio.run(main())



