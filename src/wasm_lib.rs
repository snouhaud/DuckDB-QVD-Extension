#![allow(special_module_name)]

mod lib;

// La cible Wasm exige un crate-type `staticlib`, différent du `cdylib`
// utilisé en natif. Il n'existe pas (encore) de moyen de choisir le
// crate-type selon la cible : ce fichier réexporte donc `lib` en tant
// qu'« example » pour pouvoir le compiler en staticlib.
//
// Build Wasm explicite :
//   cargo build --example qvd
