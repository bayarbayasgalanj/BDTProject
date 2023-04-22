package Data;

import java.io.Serializable;

public class TeamInfo  implements Serializable
{
	
	private static final long serialVersionUID = 1L;
	
	
	private String TeamID;
	private String Season;
	private String SeasonType;
	private String Key;
	private String City;
	private String Name;
	private String Conference;
	private String ConferenceRank;
	private String Wins;
	private String Losses;
	
	public TeamInfo( ){
		
	}
	
	public TeamInfo (String c1, String c2, String c3, String c4, String c5, String c6, String c7, String c8, String c9, String c10) {
		this.TeamID = c1;
		this.Season = c2;
		this.SeasonType = c3;
		this.Key = c4;
		this.City = c5;
		this.Name = c6;
		this.Conference = c7;
		this.ConferenceRank = c8;
		this.Wins = c9;
		this.Losses = c10;
	}
	public String getTeamID() {
		return TeamID;
	}

	public void setTeamID(String teamID) {
		TeamID = teamID;
	}

	public String getSeason() {
		return Season;
	}

	public void setSeason(String season) {
		Season = season;
	}

	public String getSeasonType() {
		return SeasonType;
	}

	public void setSeasonType(String seasonType) {
		SeasonType = seasonType;
	}

	public String getKey() {
		return Key;
	}

	public void setKey(String key) {
		Key = key;
	}

	public String getCity() {
		return City;
	}

	public void setCity(String city) {
		City = city;
	}

	public String getName() {
		return Name;
	}

	public void setName(String name) {
		Name = name;
	}

	public String getConference() {
		return Conference;
	}

	public void setConference(String conference) {
		Conference = conference;
	}

	public String getConferenceRank() {
		return ConferenceRank;
	}

	public void setConferenceRank(String conferenceRank) {
		ConferenceRank = conferenceRank;
	}

	public String getWins() {
		return Wins;
	}

	public void setWins(String wins) {
		Wins = wins;
	}

	public String getLosses() {
		return Losses;
	}

	public void setLosses(String losses) {
		Losses = losses;
	}

	//	public String getCounter (){
//		return this.counter;
//	}
//	
//	public String getCompetition(){
//		return this.competition;
//	}
//	
//	public String getMatchDay (){
//		return this.matchDay;
//	}
//	
//	public String getHomeTeam(){
//		return this.homeTeam;
//	}
//	
//	public String getAwayTeam(){
//		return this.awayTeam;
//	}
//	
//	public String getScheduledDate (){
//		return this.scheduledDate;
//	}
//	public String getScheduledDay (){
//		return this.scheduledDate.substring(0, 10);
//	}
//	public String toString(){
//		return this.counter + " , " + this.competition + " , " + this.matchDay + " , " + this.homeTeam + " , " + this.awayTeam + " , " + this.scheduledDate; 
//	}
//	
	public String toString(){
		return this.TeamID + " , " + this.Key + " , " + this.City + " , " + this.Name + " , " + this.ConferenceRank; 
	}

}

