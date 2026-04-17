import {useMemo, useState} from "react";
import {
    ResponsiveContainer,
    BarChart,
    Bar,
    XAxis,
    YAxis,
    CartesianGrid,
    Tooltip,
    Legend
    } from "recharts";
const DEFAULT_STATS = ["PPG", "RPG", "APG", "FG%", "3PT%", "FT%"];

function toNumber(value) {
    if(value == null || value == undefined || value == "") return null;
    const cleaned = String(value).replace(/[%,$]/g, "").trim();
    const num = Number(cleaned);
    return Number.isFinite(num) ? num : null
    }

export default function PlayerComparisonTab({
    teamLabel,
    seasonPlayersData
    }) {
        const rows = seasonPlayersData?.rows || [];
        const columns = seasonPlayersData?.rows || [];

        const [player1, setPlayer1] = useState("");
        const [player2, setPlayer2] = useState("");

        const availPlayers = useMemo(() => {
            return rows
            .map((row) => row.Player)
            .filter(Boolean)
            .sort((a,b) => a.localeCompare(b));
            }, [rows]);

        const availStats = useMemo (() => {
            return DEFAULT_STATS.filter((stat) => columns.includes(stat));
            }, [columns]);

        const row1 = useMemo (
            () => rows.find((row) => row.Player === player1),
            [rows, player1]
            );

        const row2 = useMemo (
            () => rows.find((row) => row.Player === player2),
            [rows, player2]
            );

        const chartData = useMemo(() => {
            if (!row1 || !row2) return [];

            return availStats
            .map((stat) => {
                const p1 = toNumber(row1[stat]);
                const p2 = toNumber(row2[stat]);

                return {
                    stat,
                    [player1]: p1,
                    [player2]: p2,
                    diff: Math.abs((p1 ?? 0) - (p2 ?? 0))
                    };
                })
                .filter((item) => item[player1] !== null || item[player2] !== null)
                .sort((a,b) => b.diff - a.diff);
            }, [row1, row2, availStats, player1, player2]);

        return (
            <div>
                <div className = "table-status">
                    <span>Compare two players for {teamLabel}</span>
                </div>

                <div className = "comparison-controls">
                    <label>
                        Player 1
                        <select value = {player1} onChange= {(e) => setPlayer(e.target.value)}>
                            <option value = "">Select Player</option>
                            {availPlayers.map((name) => (
                                <option key = {'p1_${name}'} value = {name}>
                                    {name}
                                </option>
                                ))}
                            </select>
                        </label>

                    <label>
                        Player 2
                        <select value = {player2} onChange = {(e) => setPlayer(e.target.value)}>
                            <option value = "">SelectPlayer</option>
                            {availPlayers.map((name) => (
                                <option key = {'p2_${name}'} value = {name}>
                                    {name}
                                </option>
                                ))}
                        </select>
                    </label>
                </div>

                {!player1 || !player2 ? (
                    <div className = "table-status">
                        <span>Select two players to view graph.</span>
                    </div>
                    ) : (
                        <>
                            <div style = {{width: "100%", height: 420, marginTop: "12px"}}>
                                <ResponsiveContainer>
                                    <BarChart data = {chartData}>
                                        <CartesianGrid strokeDasharray = "3 3" />
                                        <XAxis datakey = "stat" />
                                        <YAxis />
                                        <Tooltip />
                                        <Legend />
                                        <Bar datakey = {player1} />
                                        <Bar datakey = {player2} />
                                    </BarChart>
                                </ResponsiveContainer>
                            </div>

                            <div className = "table-status" style = {{ marginTop: "12px"}}>
                                <span>
                                    Stats by biggest difference between {player1} and {player2}
                                </span>
                            </div>
                        </>

                );
            </div>

        );
    }
