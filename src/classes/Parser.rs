use std::{iter::Peekable, str::Chars};

pub struct Parser {}

impl Parser {
    fn read_until_next_line(&mut self, chars: &mut Peekable<Chars<'_>>) -> String {
        let mut res = String::new();
        while let Some(&c) = chars.peek() {
            if c == '\r' {
                break;
            }
            res.push(c);
            chars.next();
        }
        return res;
    }

    fn read_n_characters(&mut self, chars: &mut Peekable<Chars<'_>>, n: u32) -> String {
        let mut res = String::new();
        for _i in 0..n {
            res.push(chars.next().unwrap());
        }
        return res;
    }

    pub fn parse(&mut self, input: &str) -> Vec<String> {
        let mut chars = input.chars().peekable();
        let mut commands = Vec::new();
        
        while let Some(ch) = chars.next() {
            if ch == '*' {
                let command_size_as_string = self.read_until_next_line(&mut chars);
                let _command_size = command_size_as_string.parse::<u32>().unwrap();
            } else if ch == '$' {
                let bulk_string_size_as_string = self.read_until_next_line(&mut chars);
                let bulk_string_size = bulk_string_size_as_string.parse::<u32>().unwrap();
                chars.next();
                chars.next();
                let simple_string = self.read_n_characters(&mut chars, bulk_string_size);
                chars.next();
                chars.next();
                commands.push(simple_string);
            }
        }
        return commands;
    }
}
