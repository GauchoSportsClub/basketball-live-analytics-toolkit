def generate_report_string(home_team, away_team, home_score, away_score):
    margin = home_score - away_score
    
    latex_report = f"""
    \\documentclass{{article}}
    \\begin{{document}}
    \\section*{{Post-Game Summary}}
    The final score differential was calculated as:
    $$ \\Delta = S_{{home}} - S_{{away}} $$
    $$ \\Delta = {margin} $$
    \\end{{document}}
    """
    
    with open("post_game_report.tex", "w") as f:
        f.write(latex_report)
