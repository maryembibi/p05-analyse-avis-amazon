#!/usr/bin/env python3
"""
P05 - Etape 2 : preparateur controle (bronze -> silver).

Lit Appliances.jsonl.gz en streaming et produit, pour un lot de produits choisi
de facon deterministe (hash de parent_asin) :

  silver/<run_id>/tokens/part-00000.tsv   review_key \t parent_asin \t tok1 tok2 ...   (entree MR1)
  silver/<run_id>/notes/part-00000.tsv    review_key \t parent_asin \t AAAA-MM \t note  (entree MR3)
  silver/<run_id>/anomalies.tsv           journal des rejets et doublons (sans user_id)
  silver/<run_id>/manifeste.json          parametres, compteurs, N, hashes -> tracabilite

Aucune statistique analytique n'est calculee ici : frequences, TF-IDF et tendances
sont faites par les jobs MapReduce Java (exigence du cahier des charges).

Usage :
  py preparer_silver.py Appliances.jsonl.gz --pourcentage 5      # lot de developpement
  py preparer_silver.py Appliances.jsonl.gz --pourcentage 20     # lot de validation
  py preparer_silver.py Appliances.jsonl.gz --pourcentage 100    # corpus complet

Bibliotheque standard Python 3.8+ uniquement.
"""
import argparse
import gzip
import hashlib
import html
import json
import os
import re
import sys
import unicodedata
from collections import Counter
from datetime import datetime, timezone

VERSION_PREPARATEUR = "prep-v1"

# --------------------------------------------------------------------------
# Regles NLP versionnees (toute modification => changer NLP_VERSION et regenerer)
# --------------------------------------------------------------------------
NLP_VERSION = "nlp-v1"

NEGATIONS = {"no", "not", "nor", "never", "none", "nothing", "neither", "nobody", "nowhere"}

STOP_WORDS = {
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any",
    "are", "as", "at", "be", "because", "been", "before", "being", "below", "between",
    "both", "but", "by", "can", "could", "did", "do", "does", "doing", "down", "during",
    "each", "few", "for", "from", "further", "had", "has", "have", "having", "he", "her",
    "here", "hers", "herself", "him", "himself", "his", "how", "i", "if", "in", "into",
    "is", "it", "its", "itself", "just", "me", "more", "most", "my", "myself", "of", "off",
    "on", "once", "only", "or", "other", "our", "ours", "ourselves", "out", "over", "own",
    "same", "she", "should", "so", "some", "such", "than", "that", "the", "their",
    "theirs", "them", "themselves", "then", "there", "these", "they", "this", "those",
    "through", "to", "too", "under", "until", "up", "very", "was", "we", "were", "what",
    "when", "where", "which", "while", "who", "whom", "why", "will", "with", "would",
    "you", "your", "yours", "yourself", "yourselves", "also", "get", "got", "one", "us",
}

PARAMS_NLP = {
    "version": NLP_VERSION,
    "champs_texte": "title + ' ' + text",
    "unicode": "html.unescape, NFKD puis suppression des accents, minuscules",
    "nettoyage": "balises HTML (<br />) et URL remplacees par des espaces",
    "contractions": "won't->will not, can't/cannot->can not, n't->not ; 's 're 've 'll 'd 'm supprimes",
    "tokenisation": "regex [a-z0-9]+ (caracteres non latins = separateurs)",
    "negations": "conservees : " + ", ".join(sorted(NEGATIONS)),
    "stop_words": f"liste anglaise interne ({len(STOP_WORDS)} mots, negations exclues)",
    "nombres": "tokens purement numeriques supprimes ; alphanumeriques (ex. references) conserves",
    "longueur_min": 2,
    "longueur_max": 30,
    "expressions_composees": "non traitees en v1 (unigrammes uniquement)",
}

RE_HTML = re.compile(r"<[^>]+>")
RE_URL = re.compile(r"https?://\S+|www\.\S+")
RE_NT = re.compile(r"n't\b")
RE_SUFFIXES = re.compile(r"'(s|re|ve|ll|d|m)\b")
RE_TOKEN = re.compile(r"[a-z0-9]+")


def tokeniser(texte):
    t = html.unescape(texte)
    t = t.replace("’", "'").replace("‘", "'")
    t = unicodedata.normalize("NFKD", t)
    t = "".join(c for c in t if not unicodedata.combining(c)).lower()
    t = RE_HTML.sub(" ", t)
    t = RE_URL.sub(" ", t)
    t = t.replace("won't", "will not").replace("can't", "can not").replace("cannot", "can not")
    t = RE_NT.sub(" not", t)
    t = RE_SUFFIXES.sub(" ", t)
    t = t.replace("'", " ")
    tokens = []
    for tok in RE_TOKEN.findall(t):
        if tok.isdigit():
            continue
        if tok in NEGATIONS:
            tokens.append(tok)
            continue
        if len(tok) < PARAMS_NLP["longueur_min"] or len(tok) > PARAMS_NLP["longueur_max"]:
            continue
        if tok in STOP_WORDS:
            continue
        tokens.append(tok)
    return tokens


# --------------------------------------------------------------------------
# Selection deterministe du lot (jamais les premieres lignes)
# --------------------------------------------------------------------------
def produit_dans_lot(parent_asin, pourcentage):
    """md5 (stable d'une execution a l'autre, contrairement a hash() de Python)."""
    if pourcentage >= 100:
        return True
    seau = int(hashlib.md5(parent_asin.encode("utf-8")).hexdigest()[:8], 16) % 10000
    return seau < pourcentage * 100


def cle_avis(avis, texte_brut):
    """Identifiant technique stable : empreinte de user_id|parent_asin|timestamp|texte.
    64 bits -> probabilite de collision ~1e-7 pour 2,1 M avis (documentee)."""
    brut = f"{avis.get('user_id')}|{avis.get('parent_asin')}|{avis.get('timestamp')}|{texte_brut}"
    return hashlib.blake2b(brut.encode("utf-8"), digest_size=8).hexdigest()


def sha256_fichier(chemin):
    h = hashlib.sha256()
    with open(chemin, "rb") as f:
        for bloc in iter(lambda: f.read(1 << 20), b""):
            h.update(bloc)
    return h.hexdigest()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("fichier", help="Appliances.jsonl.gz (zone bronze)")
    p.add_argument("--pourcentage", type=float, default=5.0, help="part des produits retenus (defaut 5)")
    p.add_argument("--sortie", default="silver", help="dossier racine des sorties")
    p.add_argument("--run-id", default=None, help="identifiant du run (defaut : genere)")
    p.add_argument("--max-lines", type=int, default=None, help="limiter pour un test")
    args = p.parse_args()

    if not os.path.exists(args.fichier):
        sys.exit(f"Fichier introuvable : {args.fichier}")
    if not (0 < args.pourcentage <= 100):
        sys.exit("--pourcentage doit etre compris entre 0 (exclu) et 100")

    debut = datetime.now(timezone.utc)
    pct_txt = f"{args.pourcentage:g}".replace(".", "_")
    run_id = args.run_id or f"lot{pct_txt}pct_{NLP_VERSION}_{debut.strftime('%Y%m%d-%H%M%S')}"
    dossier = os.path.join(args.sortie, run_id)
    if os.path.exists(dossier):
        sys.exit(f"Le dossier {dossier} existe deja : aucun ecrasement silencieux. Choisis un autre --run-id.")
    os.makedirs(os.path.join(dossier, "tokens"))
    os.makedirs(os.path.join(dossier, "notes"))

    f_tok = open(os.path.join(dossier, "tokens", "part-00000.tsv"), "w", encoding="utf-8", newline="\n")
    f_not = open(os.path.join(dossier, "notes", "part-00000.tsv"), "w", encoding="utf-8", newline="\n")
    f_ano = open(os.path.join(dossier, "anomalies.tsv"), "w", encoding="utf-8", newline="\n")
    f_ano.write("ligne\tmotifs\treview_key\n")

    lues = hors_lot = rejetes = doublons = retenus = sans_token = 0
    rejets = Counter()
    vus = set()
    produits_tokens, produits_notes = set(), set()
    vocabulaire = set()
    total_tokens = 0
    mois_min = mois_max = None

    with gzip.open(args.fichier, "rt", encoding="utf-8", errors="replace") as f:
        for num, ligne in enumerate(f, start=1):
            if args.max_lines and lues >= args.max_lines:
                break
            lues += 1
            if lues % 200_000 == 0:
                print(f"  ... {lues:,} lignes lues, {retenus:,} avis retenus", file=sys.stderr)

            try:
                avis = json.loads(ligne)
                if not isinstance(avis, dict):
                    raise ValueError
            except ValueError:
                rejetes += 1
                rejets["json_invalide"] += 1
                f_ano.write(f"{num}\tjson_invalide\t\n")
                continue

            pa = avis.get("parent_asin")
            if not isinstance(pa, str) or not pa.strip():
                rejetes += 1
                rejets["parent_asin_absent"] += 1
                f_ano.write(f"{num}\tparent_asin_absent\t\n")
                continue
            pa = pa.strip()

            if not produit_dans_lot(pa, args.pourcentage):
                hors_lot += 1
                continue

            motifs = []
            note = avis.get("rating")
            if not isinstance(note, (int, float)) or isinstance(note, bool) or not (1 <= note <= 5):
                motifs.append("note_hors_1_5")

            ts = avis.get("timestamp")
            mois = None
            if isinstance(ts, (int, float)) and not isinstance(ts, bool) and ts > 0:
                try:
                    secondes = ts / 1000 if ts > 1e11 else ts
                    mois = datetime.fromtimestamp(secondes, tz=timezone.utc).strftime("%Y-%m")
                except (OverflowError, OSError, ValueError):
                    mois = None
            if mois is None:
                motifs.append("date_non_decodable")

            texte_brut = f"{avis.get('title') or ''} {avis.get('text') or ''}".strip()
            if not texte_brut:
                motifs.append("texte_vide")

            if motifs:
                rejetes += 1
                for m in motifs:
                    rejets[m] += 1
                f_ano.write(f"{num}\t{','.join(motifs)}\t\n")
                continue

            cle = cle_avis(avis, texte_brut)
            if cle in vus:
                doublons += 1
                f_ano.write(f"{num}\tdoublon\t{cle}\n")
                continue
            vus.add(cle)

            retenus += 1
            produits_notes.add(pa)
            f_not.write(f"{cle}\t{pa}\t{mois}\t{float(note):.1f}\n")
            mois_min = mois if mois_min is None or mois < mois_min else mois_min
            mois_max = mois if mois_max is None or mois > mois_max else mois_max

            tokens = tokeniser(texte_brut)
            if not tokens:
                sans_token += 1
                f_ano.write(f"{num}\taucun_token_apres_nlp\t{cle}\n")
                continue
            produits_tokens.add(pa)
            total_tokens += len(tokens)
            vocabulaire.update(tokens)
            f_tok.write(f"{cle}\t{pa}\t{' '.join(tokens)}\n")

    f_tok.close()
    f_not.close()
    f_ano.close()

    fin = datetime.now(timezone.utc)
    fichiers = {
        "tokens/part-00000.tsv": os.path.join(dossier, "tokens", "part-00000.tsv"),
        "notes/part-00000.tsv": os.path.join(dossier, "notes", "part-00000.tsv"),
        "anomalies.tsv": os.path.join(dossier, "anomalies.tsv"),
    }
    manifeste = {
        "run_id": run_id,
        "preparateur": VERSION_PREPARATEUR,
        "debut_utc": debut.isoformat(timespec="seconds"),
        "duree_secondes": round((fin - debut).total_seconds(), 1),
        "source": {
            "fichier": os.path.basename(args.fichier),
            "taille_octets": os.path.getsize(args.fichier),
            "sha256": sha256_fichier(args.fichier),
        },
        "selection": {
            "regle": "md5(parent_asin)[:8] en entier % 10000 < pourcentage * 100",
            "pourcentage_produits": args.pourcentage,
            "max_lines": args.max_lines,
        },
        "regle_dedoublonnage": "blake2b-64(user_id|parent_asin|timestamp|title+text) ; user_id non conserve",
        "parametres_nlp": PARAMS_NLP,
        "compteurs": {
            "lignes_lues": lues,
            "hors_lot": hors_lot,
            "rejetes": rejetes,
            "rejets_par_motif": dict(rejets),
            "doublons": doublons,
            "avis_retenus_notes": retenus,
            "avis_sans_token_apres_nlp": sans_token,
            "avis_retenus_tokens": retenus - sans_token,
            "total_tokens": total_tokens,
            "taille_vocabulaire": len(vocabulaire),
        },
        "N_produits_avec_document_textuel": len(produits_tokens),
        "produits_flux_notes": len(produits_notes),
        "periode_mois": {"min": mois_min, "max": mois_max},
        "sorties_sha256": {nom: sha256_fichier(chemin) for nom, chemin in fichiers.items()},
    }
    with open(os.path.join(dossier, "manifeste.json"), "w", encoding="utf-8") as f:
        json.dump(manifeste, f, ensure_ascii=False, indent=2)

    c = manifeste["compteurs"]
    print(f"\n=== PREPARATION TERMINEE : {run_id} ===")
    print(f"Lignes lues               : {c['lignes_lues']:,}")
    print(f"Hors lot ({args.pourcentage:g} % produits) : {c['hors_lot']:,}")
    print(f"Rejetes                   : {c['rejetes']:,}  {c['rejets_par_motif']}")
    print(f"Doublons                  : {c['doublons']:,}")
    print(f"Avis retenus (notes)      : {c['avis_retenus_notes']:,}")
    print(f"Avis retenus (tokens)     : {c['avis_retenus_tokens']:,}  (sans token : {c['avis_sans_token_apres_nlp']:,})")
    print(f"N produits (pour MR2)     : {manifeste['N_produits_avec_document_textuel']:,}")
    print(f"Tokens / vocabulaire      : {c['total_tokens']:,} / {c['taille_vocabulaire']:,}")
    print(f"Periode                   : {mois_min} -> {mois_max}")
    print(f"Duree                     : {manifeste['duree_secondes']} s")
    print(f"\nSorties dans : {dossier}")


if __name__ == "__main__":
    main()
