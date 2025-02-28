from translate import Translator
import re
import time

def clean_text(text):
    """Remove brackets and their contents"""
    return re.sub(r'\[.*?\]', '', text)

def split_text(text, max_length=450):  # Reduced to 450 for safety margin
    """Split text into chunks guaranteed to be under max_length"""
    text = clean_text(text)
    chunks = []
    current_chunk = []
    current_length = 0
    
    # Split into words to avoid breaking words
    words = text.split()
    for word in words:
        if current_length + len(word) + 1 > max_length:  # +1 for space
            chunks.append(' '.join(current_chunk))
            current_chunk = [word]
            current_length = len(word)
        else:
            current_chunk.append(word)
            current_length += len(word) + 1
    
    if current_chunk:
        chunks.append(' '.join(current_chunk))
    
    return chunks

def translate_file(input_file, output_file, target_lang):
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            text = f.read()
        
        chunks = split_text(text)
        translator = Translator(from_lang='hi', to_lang=target_lang, provider='mymemory')
        
        translated = []
        for i, chunk in enumerate(chunks):
            # Skip already processed chunks if resuming
            if i < len(translated):
                continue
                
            success = False
            retries = 3
            for attempt in range(retries):
                try:
                    print(f"\nTranslating chunk {i+1}/{len(chunks)} ({len(chunk)} chars) - Attempt {attempt+1}")
                    
                    # Remove timeout parameter and use general exception handling
                    translated_chunk = translator.translate(chunk)
                    
                    # Verify translation quality
                    if translated_chunk.strip() == chunk.strip():
                        raise ValueError("No translation occurred")
                        
                    translated.append(translated_chunk)
                    success = True
                    break
                
                except Exception as e:
                    print(f"Translation error: {str(e)}")
                    if "QUERY LENGTH LIMIT" in str(e):
                        print("Implementing dynamic chunk reduction...")
                        new_chunks = split_text(chunk, max_length=int(len(chunk)*0.8))
                        chunks[i+1:i+1] = new_chunks
                        break
                    if attempt < retries - 1:
                        wait_time = 2 ** attempt
                        print(f"Retrying in {wait_time} seconds...")
                        time.sleep(wait_time)
                        continue
                    else:
                        print("Final failure - skipping chunk")
                        translated.append(f"[TRANSLATION FAILED: {str(e)}]")
                        break
            
            # Save progress after each chunk
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(' '.join(translated))
            print(f"Progress saved: {i+1}/{len(chunks)} chunks completed")
            
        print(f"Successfully translated to {output_file}")
    
    except Exception as e:
        print(f"Critical translation failure: {str(e)}")

def main():
    # File paths
    file_with_timestamps = "Jaan Se Pyara Juni - Mega Last Ep 34 - Part 02 - [CC]  25 Dec 2024, PWRD By Happilac Paints - HUM TV_with_timestamps.txt"
    file_without_timestamps = "Jaan Se Pyara Juni - Mega Last Ep 34 - Part 02 - [CC]  25 Dec 2024, PWRD By Happilac Paints - HUM TV_without_timestamps.txt"

    # Translate to English
    translate_file(file_with_timestamps, "video_title_with_timestamps_en.txt", 'en')
    translate_file(file_without_timestamps, "video_title_without_timestamps_en.txt", 'en')

    # Translate to Urdu
    translate_file(file_with_timestamps, "video_title_with_timestamps_ur.txt", 'ur')
    translate_file(file_without_timestamps, "video_title_without_timestamps_ur.txt", 'ur')

if __name__ == "__main__":
    main() 