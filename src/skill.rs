//! The bundled agent skill: `chattrs skill` prints this text to stdout.
//!
//! The text is embedded at compile time, so every host with the binary has
//! the skill. It opens no connection and needs no address.

/// The full text of `skills/chattrs/SKILL.md`.
pub const SKILL: &str = include_str!("../skills/chattrs/SKILL.md");

/// The text printed by `chattrs skill`.
pub fn text() -> &'static str {
    SKILL
}

#[cfg(test)]
mod tests {
    use super::*;

    fn skill_path() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("skills/chattrs/SKILL.md")
    }

    #[test]
    fn skill_command_prints_embedded_skill() {
        let on_disk = std::fs::read_to_string(skill_path()).unwrap();
        assert_eq!(text(), on_disk);
        assert!(text().lines().count() <= 60, "skill exceeds 60 lines");
    }

    #[tokio::test]
    async fn skill_command_needs_no_broker() {
        let args = crate::cli::Args { command: crate::cli::Command::Skill };
        let settings = crate::settings::Settings::resolve(&|_| None, &Default::default());
        crate::run(args, &settings).await.unwrap();
    }

    #[test]
    fn skill_names_send_ask_reply_and_no_polling() {
        for needle in ["chattrs send", "chattrs ask", "chattrs reply", "Do not poll"] {
            assert!(text().contains(needle), "skill is missing {needle:?}");
        }
    }
}
