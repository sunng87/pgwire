{
  description = "Development environment flake";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
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
          psycopg
          psycopg-c
          psycopg2
        ]);
        buildInputs = with pkgs; [
          llvmPackages.libclang
          sqlite.dev
          sqlite.out
          openssl
          libpq.dev
          libpq.out
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
            jbang
            jdk_headless
            gnumake
            openssl
          ];


          buildInputs = buildInputs;

          shellHook = ''
            export CC=clang
            export CXX=clang++
            export SQLITE3_LIB_DIR="${pkgs.sqlite.dev}/lib"
            export SQLITE3_INCLUDE_DIR="${pkgs.sqlite.dev}/include"

            export OPENSSL_DIR="${pkgs.openssl.dev}"
            export OPENSSL_LIB_DIR="${pkgs.openssl.out}/lib"
            export OPENSSL_INCLUDE_DIR="${pkgs.openssl.dev}/include"

            export LD_LIBRARY_PATH="${pkgs.openssl.out}/lib:$LD_LIBRARY_PATH"
            export LIBRARY_PATH="${pkgs.openssl.out}/lib:$LIBRARY_PATH"
          '';
        };
      });
}
