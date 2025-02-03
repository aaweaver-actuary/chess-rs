// src/main.rs

use std::fs;
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;
use std::str::FromStr;

use anyhow::{anyhow, Result};
use chrono::{NaiveDate, NaiveTime};
use futures::stream::{FuturesUnordered, StreamExt};
use polars::prelude::*;
use rayon::prelude::*;
use regex::Regex;
use reqwest::Client;
use tokio::fs as async_fs;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

use chess_rs::{
    extract_game_type_from_event_string, extract_termination_type,
    extract_winner_from_result_string, ChessGame, GameType, TerminationType, TimeControl, Winner,
};

/// Parse a single PGN game block into a [`ChessGame`] struct.
///
/// # Arguments
///
/// * `pgn_text` - A string slice containing one PGN game (its headers and optionally moves).
///
/// # Returns
///
/// * `Some(ChessGame)` if the required headers were found and parsed; otherwise, `None`.
pub fn parse_pgn_game(pgn_text: &str) -> Option<ChessGame> {
    // Use a regex to extract header lines.
    let re = Regex::new(r#"^\[(\w+)\s+"([^"]+)"\]"#).unwrap();
    let mut headers = std::collections::HashMap::new();

    for line in pgn_text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some(caps) = re.captures(line) {
            let key = caps.get(1)?.as_str();
            let value = caps.get(2)?.as_str();
            headers.insert(key, value);
        }
    }

    // Required headers.
    let event = headers.get("Event")?;
    let game_type = extract_game_type_from_event_string(event);
    let website = headers.get("Site")?;
    let white_player_name = headers.get("White")?;
    let black_player_name = headers.get("Black")?;
    let white_elo: i32 = headers.get("WhiteElo")?.parse().ok()?;
    let black_elo: i32 = headers.get("BlackElo")?.parse().ok()?;
    let time_control = TimeControl::from_str(headers.get("TimeControl")?).ok()?;
    let result = headers.get("Result")?;
    let utc_date_str = headers.get("UTCDate")?;
    let utc_time_str = headers.get("UTCTime")?;
    let opening = headers.get("Opening")?;
    let eco = headers.get("ECO")?;

    // Determine if the game is rated. (If the event string contains "unrated" then false.)
    let rated = !event.to_lowercase().contains("unrated");

    // Determine winner and termination type.
    let winner = extract_winner_from_result_string(result);
    let termination_type = extract_termination_type(headers.get("Termination")?);

    // Parse date and time if available.
    let date = if *utc_date_str == "????.??.??" {
        None
    } else {
        NaiveDate::parse_from_str(utc_date_str, "%Y.%m.%d").ok()
    };

    let time = if *utc_time_str == "??:??:??" {
        None
    } else {
        NaiveTime::parse_from_str(utc_time_str, "%H:%M:%S").ok()
    };

    Some(ChessGame::builder()
        .rated(rated)
        .url(website.to_string())
        .game_type(game_type)
        .time_control(time_control)
        .white_player_name(white_player_name.to_string())
        .white_player_elo(white_elo as u32)
        .black_player_name(black_player_name.to_string())
        .black_player_elo(black_elo as u32)
        .rating_diff((white_elo - black_elo).abs() as i32)
        .winner(winner)
        .termination_type(termination_type)
        .date(date)
        .time(time)
        .opening_name(opening.to_string())
        .opening_eco(eco.to_string())
        .game_id(Uuid::new_v4().to_string())
        .build()
        .expect("Failed to build ChessGame"))
}

/// Download a file asynchronously from a URL and save it to `output_path`.
///
/// # Arguments
///
/// * `url` - The URL of the file to download.
/// * `output_path` - The path where the file will be saved.
pub async fn download_file(url: &str, output_path: &str) -> Result<()> {
    let client = Client::new();
    let response = client.get(url).send().await?;
    if !response.status().is_success() {
        return Err(anyhow!(
            "Download failed with status: {}",
            response.status()
        ));
    }
    // Stream the response bytes and write them to file.
    let mut stream = response.bytes_stream();
    let mut file = async_fs::File::create(output_path).await?;
    while let Some(chunk) = stream.next().await {
        let data = chunk?;
        file.write_all(&data).await?;
    }
    Ok(())
}

/// Decompress a Zstandard-compressed file.
///
/// # Arguments
///
/// * `input_path` - The path to the compressed (.zst) file.
/// * `output_path` - The path where the decompressed file is written.
pub fn decompress_zst_file(input_path: &str, output_path: &str) -> Result<()> {
    let input_file = fs::File::open(input_path)?;
    let mut reader = BufReader::new(input_file);
    let output_file = fs::File::create(output_path)?;
    let mut writer = BufWriter::new(output_file);
    zstd::stream::copy_decode(&mut reader, &mut writer)?;
    writer.flush()?;
    Ok(())
}

/// Parse a PGN file into a vector of [`ChessGame`] objects.
///
/// This function reads the entire PGN file into memory, splits the text on occurrences
/// of `"[Event "` (re-adding the tag marker), and then uses Rayon to parse each game in parallel.
///
/// # Arguments
///
/// * `pgn_path` - The path to the PGN file.
///
/// # Returns
///
/// * A vector of parsed `ChessGame` objects.
pub fn parse_pgn_file(pgn_path: &str) -> Result<Vec<ChessGame>> {
    let content = fs::read_to_string(pgn_path)?;
    // Split the file by occurrences of "[Event " and re-add the missing "[Event " to each block.
    let games: Vec<String> = content
        .split("[Event ")
        .skip(1)
        .map(|s| format!("[Event {}", s))
        .collect();
    // Process games in parallel.
    let parsed_games: Vec<ChessGame> = games
        .par_iter()
        .filter_map(|game_text| parse_pgn_game(game_text))
        .collect();
    Ok(parsed_games)
}

// /// Write a slice of [`ChessGame`] objects to a Parquet file using Polars.
// ///
// /// # Arguments
// ///
// /// * `games` - A slice of `ChessGame` objects.
// /// * `output_path` - The path for the output Parquet file.
// pub fn write_games_to_parquet(games: &[ChessGame], output_path: &str) -> Result<()> {
//     // Create vectors for each column.
//     let mut rated_vec = Vec::with_capacity(games.len());
//     let mut url_vec = Vec::with_capacity(games.len());
//     let mut game_type_vec = Vec::with_capacity(games.len());
//     let mut white_player_name_vec = Vec::with_capacity(games.len());
//     let mut white_player_elo_vec = Vec::with_capacity(games.len());
//     let mut black_player_name_vec = Vec::with_capacity(games.len());
//     let mut black_player_elo_vec = Vec::with_capacity(games.len());
//     let mut rating_diff_vec = Vec::with_capacity(games.len());
//     let mut winner_vec = Vec::with_capacity(games.len());
//     let mut termination_type_vec = Vec::with_capacity(games.len());
//     let mut date_vec = Vec::with_capacity(games.len());
//     let mut time_vec = Vec::with_capacity(games.len());
//     let mut opening_name_vec = Vec::with_capacity(games.len());
//     let mut opening_eco_vec = Vec::with_capacity(games.len());
//     let mut game_id_vec = Vec::with_capacity(games.len());

//     for game in games {
//         rated_vec.push(game.rated);
//         url_vec.push(game.url.clone());
//         game_type_vec.push(game.game_type.clone());
//         white_player_name_vec.push(game.white_player_name.clone());
//         white_player_elo_vec.push(game.white_player_elo);
//         black_player_name_vec.push(game.black_player_name.clone());
//         black_player_elo_vec.push(game.black_player_elo);
//         rating_diff_vec.push(game.rating_diff);
//         winner_vec.push(game.winner.clone());
//         termination_type_vec.push(game.termination_type.clone());
//         // For simplicity, dates and times are stored as strings.
//         date_vec.push(game.date.map(|d| d.format("%Y-%m-%d").to_string()));
//         time_vec.push(game.time.map(|t| t.format("%H:%M:%S").to_string()));
//         opening_name_vec.push(game.opening_name.clone());
//         opening_eco_vec.push(game.opening_eco.clone());
//         game_id_vec.push(game.game_id.clone());
//     }

//     // Build Series.
//     let s_rated = Series::new("rated", rated_vec);
//     let s_url = Series::new("url", url_vec);
//     let s_game_type = Series::new("game_type", game_type_vec);
//     let s_white_player_name = Series::new("white_player_name", white_player_name_vec);
//     let s_white_player_elo = Series::new("white_player_elo", white_player_elo_vec);
//     let s_black_player_name = Series::new("black_player_name", black_player_name_vec);
//     let s_black_player_elo = Series::new("black_player_elo", black_player_elo_vec);
//     let s_rating_diff = Series::new("rating_diff", rating_diff_vec);
//     let s_winner = Series::new("winner", winner_vec);
//     let s_termination_type = Series::new("termination_type", termination_type_vec);
//     let s_date = Series::new("date", date_vec);
//     let s_time = Series::new("time", time_vec);
//     let s_opening_name = Series::new("opening_name", opening_name_vec);
//     let s_opening_eco = Series::new("opening_eco", opening_eco_vec);
//     let s_game_id = Series::new("game_id", game_id_vec);

//     // Create the DataFrame.
//     let mut df = DataFrame::new(vec![
//         s_rated,
//         s_url,
//         s_game_type,
//         s_white_player_name,
//         s_white_player_elo,
//         s_black_player_name,
//         s_black_player_elo,
//         s_rating_diff,
//         s_winner,
//         s_termination_type,
//         s_date,
//         s_time,
//         s_opening_name,
//         s_opening_eco,
//         s_game_id,
//     ])?;

//     // Write the DataFrame to a Parquet file.
//     let file = fs::File::create(output_path)?;
//     ParquetWriter::new(file).finish(&mut df)?;
//     Ok(())
// }

/// Ensure that the folder structure for a given year and month exists.
///
/// # Arguments
///
/// * `year` - The year.
/// * `month` - The month (1–12).
pub fn ensure_folder_structure(year: i32, month: i32) -> Result<()> {
    let folder_path = format!("lichess_data/{}/{}", year, format!("{:02}", month));
    fs::create_dir_all(&folder_path)?;
    Ok(())
}

/// Construct the URL for a given year and month.
///
/// # Arguments
///
/// * `year` - The year.
/// * `month` - The month (1–12).
///
/// # Returns
///
/// * The constructed URL as a `String`.
pub fn construct_url(year: i32, month: i32) -> String {
    format!(
        "https://database.lichess.org/standard/lichess_db_standard_rated_{}-{:02}.pgn.zst",
        year, month
    )
}

/// Process the entire flow for a given year and month:
/// 1. Ensure the folder exists.
/// 2. Download the compressed file (if not already present).
/// 3. Decompress the file (if not already done).
/// 4. Parse the PGN into [`ChessGame`] objects.
/// 5. Save the data to Parquet in chunks of 100,000 games.
///
/// # Arguments
///
/// * `year` - The year.
/// * `month` - The month.
pub async fn process_year_month(year: i32, month: i32) -> Result<()> {
    ensure_folder_structure(year, month)?;
    let work_dir = format!("lichess_data/{}/{}", year, format!("{:02}", month));
    let url = construct_url(year, month);

    let compressed_path = format!("{}/{}-{:02}.pgn.zst", work_dir, year, month);
    let pgn_path = format!("{}/{}-{:02}.pgn", work_dir, year, month);

    if !Path::new(&compressed_path).exists() {
        println!("Downloading data from {} to {}", url, compressed_path);
        download_file(&url, &compressed_path).await?;
        println!("Download completed.");
    } else {
        println!("Compressed file already exists: {}", compressed_path);
    }

    if !Path::new(&pgn_path).exists() {
        println!("Decompressing {} to {}", compressed_path, pgn_path);
        decompress_zst_file(&compressed_path, &pgn_path)?;
        println!("Decompression completed.");
    } else {
        println!("Decompressed PGN file already exists: {}", pgn_path);
    }

    println!("Parsing PGN file: {}", pgn_path);
    let games = parse_pgn_file(&pgn_path)?;
    println!("Parsed {} games.", games.len());

    // Write games in chunks of 100,000.
    let chunk_size = 100_000;
    let mut file_counter = 0;
    for chunk in games.chunks(chunk_size) {
        file_counter += 1;
        let parquet_path = format!(
            "{}/{}-{:02}__{:03}.parquet",
            work_dir, year, month, file_counter
        );
        println!("Writing {} games to {}", chunk.len(), parquet_path);
        // write_games_to_parquet(chunk, &parquet_path)?;
    }

    println!("Finished processing data for {}/{}", year, month);
    Ok(())
}

/// The main function spawns asynchronous tasks for each desired year and month.
/// Years and months are filtered according to the rules:
/// - For 2013, only months >= August are processed.
/// - For 2017, only months <= April are processed.
#[tokio::main]
async fn main() -> Result<()> {
    let mut tasks = FuturesUnordered::new();

    for year in 2013..2018 {
        for month in 1..=12 {
            if year == 2013 && month < 8 {
                continue;
            }
            if year == 2017 && month > 4 {
                continue;
            }
            // Spawn a task for each year-month pair.
            let fut = process_year_month(year, month);
            tasks.push(tokio::spawn(async move {
                if let Err(e) = fut.await {
                    eprintln!("Error processing {}/{}: {:?}", year, month, e);
                }
            }));
        }
    }

    // Await all tasks.
    while (tasks.next().await).is_some() {}
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that a sample PGN game is correctly parsed.
    #[test]
    fn test_parse_pgn_game() {
        let sample = r#"[Event "Rated Bullet game"]
[Site "https://lichess.org/QSgawA0K"]
[White "ShahinMohammad"]
[Black "Drummied"]
[Result "0-1"]
[UTCDate "2014.06.30"]
[UTCTime "22:00:11"]
[WhiteElo "1525"]
[BlackElo "1458"]
[WhiteRatingDiff "-14"]
[BlackRatingDiff "+14"]
[ECO "A00"]
[Opening "Mieses Opening"]
[TimeControl "60+0"]
[Termination "Time forfeit"]

1. d3 d5 2. g3 e6 3. Bg2 Nf6"#;

        let game = parse_pgn_game(sample).expect("Failed to parse PGN game");
        assert_eq!(game.url, "https://lichess.org/QSgawA0K");
        assert_eq!(game.white_player_name, "ShahinMohammad");
        assert_eq!(game.black_player_name, "Drummied");
        assert_eq!(game.white_player_elo, 1525);
        assert_eq!(game.black_player_elo, 1458);
        assert_eq!(game.rating_diff, 67);
        assert_eq!(game.game_type, GameType::Bullet);
        assert_eq!(game.winner, Some(Winner::Black));
        assert_eq!(game.termination_type, TerminationType::TimeForfeit);
        assert_eq!(
            game.date.unwrap().format("%Y.%m.%d").to_string(),
            "2014.06.30"
        );
        assert_eq!(
            game.time.unwrap().format("%H:%M:%S").to_string(),
            "22:00:11"
        );
        assert_eq!(game.opening_name, "Mieses Opening");
        assert_eq!(game.opening_eco, "A00");
    }
}
