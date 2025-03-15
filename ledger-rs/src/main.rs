use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};

use ledger_rs_core::core::{BeanFile, BeanFileParse, Statement};

#[derive(Parser)]
#[command(version, about, long_about=None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Read { filepath: PathBuf },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Command::Read { filepath } => read(filepath),
    }
}

fn read(f: PathBuf) {
    let base = f.clone();
    let first = BeanFile::new(f);

    let mut p = BeanFileParse::new(first);
    p.statements = p.beanfile.parse();

    let include_paths: Vec<BeanFileParse> = p
        .statements
        .iter()
        .filter_map(|x| match x {
            Statement::Include(y) => Some(y),
            _ => None,
        })
        .map(|x| {
            let b = Path::new(x.path);
            match base.parent() {
                Some(p) => p.join(b),
                None => b.to_path_buf(),
            }
        })
        .map(|x| BeanFile::new(x))
        .map(|x| BeanFileParse::new(x))
        //.map(|x| format!("{}{}", base_directory, x.path))
        .collect();

    for mut i in include_paths {
        println!("{:?}", i.beanfile.filepath);
        i.statements = i.beanfile.parse();
        // i.statements
        //     .iter()
        //     .filter_map(|x| match x {
        //         Statement::Other(y) => Some(y),
        //         _ => None,
        //     })
        //     .filter(|x| **x != "")
        //     .for_each(|x| println!("{:?}", x));
        i.statements.iter().for_each(|x| println!("{:?}", x));
        println!("-----");
    }

    //include_paths.iter().for_each(|x| println!("{}", x));

    //p.statements.iter().for_each(|x| println!("{:?}", x));
}
