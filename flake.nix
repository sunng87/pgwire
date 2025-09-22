{
  description = "Development environment flake";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, fenix, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        pythonEnv = pkgs.python3.withPackages (ps: with ps; [
          psycopg-c
          psycopg2
        ]);
        buildInputs = with pkgs; [
          llvmPackages.libclang
          duckdb.dev
          duckdb.lib
          sqlite.dev
          sqlite.out
          openssl.out
          libpq
        ];
      in
      {
        devShells.default = pkgs.mkShell {
          nativeBuildInputs = with pkgs; [
            pkg-config
            clang
            git
            mold
            (fenix.packages.${system}.stable.withComponents [
              "cargo"
              "clippy"
              "rust-src"
              "rustc"
              "rustfmt"
              "rust-analyzer"
            ])
            cargo-nextest
            cargo-release
            curl
            gnuplot ## for cargo bench
            pythonEnv
            postgresql_18.out

            babashka
            nodejs_24
            go
            openssh
          ];

          LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath buildInputs;
          shellHook = ''
            export CC=clang
            export CXX=clang++
            export SQLITE3_LIB_DIR="${pkgs.sqlite.dev}/lib"
            export SQLITE3_INCLUDE_DIR="${pkgs.sqlite.dev}/include"
            export DUCKDB_LIB_DIR="${pkgs.duckdb.lib}/lib"
            export DUCKDB_INCLUDE_DIR="${pkgs.duckdb.dev}/include"
          '';
        };
      });
}
