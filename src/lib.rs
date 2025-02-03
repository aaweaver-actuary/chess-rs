use std::{fmt::{self, Display, Formatter}, str::FromStr};
use derive_builder::Builder;

use chrono::{NaiveDate, NaiveTime};

#[derive(Debug, Clone, PartialEq)]
pub enum Winner {
    White,
    Black,
}

impl FromStr for Winner {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "white" => Ok(Self::White),
            "black" => Ok(Self::Black),
            _ => Err(()),
        }
    }
}

impl Display for Winner {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Self::White => write!(f, "White"),
            Self::Black => write!(f, "Black"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TerminationType {
    Normal,
    TimeForfeit,
}

impl FromStr for TerminationType {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "normal" => Ok(Self::Normal),
            "time forfeit" => Ok(Self::TimeForfeit),
            "time" => Ok(Self::TimeForfeit),
            _ => Err(()),
        }
    }
}

impl Display for TerminationType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Self::Normal => write!(f, "Normal"),
            Self::TimeForfeit => write!(f, "Time forfeit"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum GameType {
    Bullet,
    Blitz,
    Rapid,
    Classical,
}

impl FromStr for GameType {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "bullet" => Ok(Self::Bullet),
            "blitz" => Ok(Self::Blitz),
            "rapid" => Ok(Self::Rapid),
            "classical" => Ok(Self::Classical),
            _ => Err(()),
        }
    }
}

impl Display for GameType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Self::Bullet => write!(f, "Bullet"),
            Self::Blitz => write!(f, "Blitz"),
            Self::Rapid => write!(f, "Rapid"),
            Self::Classical => write!(f, "Classical"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TimeControl(u32, u32);

impl TimeControl {
    pub fn new(minutes: u32, increment: u32) -> Self {
        Self(minutes, increment)
    }
}

impl Display for TimeControl {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}+{}", self.0, self.1)
    }
}

impl FromStr for TimeControl {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('+').collect();
        if parts.len() != 2 {
            return Err(());
        }
        let minutes = parts[0].parse().map_err(|_| ())?;
        let increment = parts[1].parse().map_err(|_| ())?;
        Ok(Self(minutes, increment))
    }
}

/// A chess game with header information.
#[derive(Debug, Clone, Builder, PartialEq)]
pub struct ChessGame {
    pub rated: bool,
    pub url: String,
    pub game_type: GameType,
    pub time_control: TimeControl,
    pub white_player_name: String,
    pub white_player_elo: u32,
    pub black_player_name: String,
    pub black_player_elo: u32,
    pub rating_diff: i32,
    /// Winner is "White" or "Black" when the result is decisive; if a draw then `None`.
    pub winner: Option<Winner>,
    /// Either `"Normal"` or `"Time forfeit"`.
    pub termination_type: TerminationType,
    pub date: Option<NaiveDate>,
    pub time: Option<NaiveTime>,
    pub opening_name: String,
    pub opening_eco: String,
    pub game_id: String,
}

impl ChessGame {
    pub fn builder() -> ChessGameBuilder {
        ChessGameBuilder::default()
    }
}

/// Extract game type (eg "Bullet", "Blitz", "Rapid", "Classical") from the event string.
pub fn extract_game_type_from_event_string(event: &str) -> GameType {
    let out: String;

    if event.to_lowercase().contains("bullet") {
        out = "Bullet".to_owned();
    } else if event.to_lowercase().contains("blitz") {
        out = "Blitz".to_owned();
    } else if event.to_lowercase().contains("rapid") {
        out = "Rapid".to_owned();
    } else if event.to_lowercase().contains("classical") {
        out = "Classical".to_owned();
    } else {
        out = format!("Unknown ({})", event).as_str().to_owned();
    }
    GameType::from_str(&out).expect("Invalid game type")
}

/// Extract winner from result string, White, Black or None for a draw.
pub fn extract_winner_from_result_string(result: &str) -> Option<Winner> {
    match result {
        "1-0" => Some(Winner::White),
        "0-1" => Some(Winner::Black),
        "1/2-1/2" => None,
        _ => None,
    }
}

pub fn extract_termination_type(termination: &str) -> TerminationType {
    match termination.to_lowercase().as_str() {
        "normal" => TerminationType::Normal,
        "time forfeit" => TerminationType::TimeForfeit,
        _ => panic!("Invalid termination type"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_winner_from_str() {
        assert_eq!(Winner::from_str("white"), Ok(Winner::White));
        assert_eq!(Winner::from_str("black"), Ok(Winner::Black));
        assert_eq!(Winner::from_str("White"), Ok(Winner::White));
        assert_eq!(Winner::from_str("Black"), Ok(Winner::Black));
        assert_eq!(Winner::from_str("invalid"), Err(()));
    }

    #[test]
    fn test_winner_display() {
        assert_eq!(format!("{}", Winner::White), "White");
        assert_eq!(format!("{}", Winner::Black), "Black");
    }

    #[test]
    fn test_termination_type_from_str() {
        assert_eq!(TerminationType::from_str("normal"), Ok(TerminationType::Normal));
        assert_eq!(TerminationType::from_str("time forfeit"), Ok(TerminationType::TimeForfeit));
        assert_eq!(TerminationType::from_str("time"), Ok(TerminationType::TimeForfeit));
        assert_eq!(TerminationType::from_str("invalid"), Err(()));
    }

    #[test]
    fn test_termination_type_display() {
        assert_eq!(format!("{}", TerminationType::Normal), "Normal");
        assert_eq!(format!("{}", TerminationType::TimeForfeit), "Time forfeit");
    }

    #[test]
    fn test_game_type_from_str() {
        assert_eq!(GameType::from_str("bullet"), Ok(GameType::Bullet));
        assert_eq!(GameType::from_str("blitz"), Ok(GameType::Blitz));
        assert_eq!(GameType::from_str("rapid"), Ok(GameType::Rapid));
        assert_eq!(GameType::from_str("classical"), Ok(GameType::Classical));
        assert_eq!(GameType::from_str("invalid"), Err(()));
    }

    #[test]
    fn test_game_type_display() {
        assert_eq!(format!("{}", GameType::Bullet), "Bullet");
        assert_eq!(format!("{}", GameType::Blitz), "Blitz");
        assert_eq!(format!("{}", GameType::Rapid), "Rapid");
        assert_eq!(format!("{}", GameType::Classical), "Classical");
    }

    #[test]
    fn test_extract_game_type_from_event_string() {
        assert_eq!(extract_game_type_from_event_string("a bullet game"), GameType::Bullet);
        assert_eq!(extract_game_type_from_event_string("Blitz"), GameType::Blitz);
        assert_eq!(extract_game_type_from_event_string("RAPID"), GameType::Rapid);
        assert_eq!(extract_game_type_from_event_string("Classical GAME"), GameType::Classical);
    }

    #[test]
    fn test_extract_winner_from_result_string() {
        assert_eq!(extract_winner_from_result_string("1-0"), Some(Winner::White));
        assert_eq!(extract_winner_from_result_string("0-1"), Some(Winner::Black));
        assert_eq!(extract_winner_from_result_string("1/2-1/2"), None);
        assert_eq!(extract_winner_from_result_string("invalid"), None);
    }
        
    #[test]
    fn test_extract_termination_type() {
        assert_eq!(extract_termination_type("normal"), TerminationType::Normal);
        assert_eq!(extract_termination_type("time forfeit"), TerminationType::TimeForfeit);
    }

    #[test]
    fn test_time_control_from_str() {
        assert_eq!(TimeControl::from_str("5+5"), Ok(TimeControl(5, 5)));
        assert_eq!(TimeControl::from_str("5+invalid"), Err(()));
        assert_eq!(TimeControl::from_str("invalid"), Err(()));
    }
   
}
