/*

This is a concordance file for HS6 level codes from the HS2007 
classification to the HS2012 classification.  The purpose of this
file is to ensure those working with Colombian or Chilean data
are able to track goods continously across the years of the data (2007-2013)
as during the time frame.

This do file operates under the assumption that all products from
2007 to 2011 use the HS2007 system, while all products from 2012 to 2013
use the HS2012 system. Future work should ensure that this assumption is
correct. If this assumption does not hold then some of the sections that are 
commented out, will need be to adjusted.

This do file uses the Pierce and Schott (2012) method, in fact it makes
use of the code generously provided by the authors.
Paper: http://www.justinrpierce.com/index_files/Pierce_Schott_JOS_2012.pdf
Files: http://faculty.som.yale.edu/peterschott/sub_international.htm
	Under "Concordance for 1989-2007 US HS codes over time"

This do file makes use of their export (schedule b) do file.

The use of Pierce and Schott (2012) implies that syntetic codes are created.

The concordance between HS2007 and HS2012 is available at:
https://unstats.un.org/unsd/trade/conversions/HS%20Correlation%20and%20Conversion%20tables.htm

Needed Input Files: 
	1) "HS 2012 to HS 2007 Correlation and conversion tables.xls"
		Via: https://unstats.un.org/unsd/trade/conversions/HS%20Correlation%20and%20Conversion%20tables.htm



Any errors are my own.

-Ryan Lee



*/

**1 Preliminaries
clear
set more off
set mem 1000m

cd "C:\Users\Ryan\Documents\FirmLevelData\Colombia\RawData\"


**1A. Add Concordance File
import excel "HS 2012 to HS 2007 Correlation and conversion tables.xls", sheet("Conversion HS12-HS07") cellrange(A2:B5207) firstrow clear
gen effyr=2012
gen seqnum=_n
replace seqnum=(seqnum*10000)
gen setyr=seqnum+effyr
drop seqnum
replace setyr=(setyr/10000)
destring HS2012, replace
destring HS2007, replace
rename HS2012 new
rename HS2007 obsolete
save "hs6concordances2007_2012.dta", replace //Due to their being only one year of change this suffices as the only concordance needed



**2 Create a file that chains years together
**
**  Note that to chain you have to always match later years to earlier years. That is the reason for the 
**  second loop below is nested.
**  Note that you must set the local variables for the beginning and ending year you want, i.e., the long
**  difference that you want to take; these locals govern both this and the next secion. 

local b  = 2007
local e  = 2013
local b1 = `b'+1

set more off
*quietly {

	*chop up the data in the main file created above year and rename the vars for
	*the merging to take place in the next loop
	forvalues y=`b'/`e' {
		use "hs6concordances2007_2012.dta", clear //This is where the concordance file goes.
		keep if effyr==`y' 
		rename new new`y'
		rename obsolete obs`y'
		rename setyr setyr`y'
		rename effyr effyr`y'
		order obs`y' new`y'
		sort obs`y' 
		save temp_xchain_`y', replace
	}

/*
THIS SECTION IS COMMENTED OUT AS THERE IS ONLY ONE YEAR WHERE THERE IS A CHANGE	
	
	*use the chopped up files from above to chain the obs-new matches across years. here, the goal is to find 
	*new's from subsequent years that modify new's from earlier years
	*
	*note that after the inside loop, which matches subsequent years to a given year, drop observations unless they 
	*are chained, i.e., unless the merge code = 3
	forvalues s=`b'/`e' {
		use temp_xchain_`s', clear
		rename obs`s' obs
		forvalues t=`b'/`e' {
			if `t'>`s' {
				noisily display [`s'] " " [`t']
				rename new`s' obs`t'
				sort obs`t'
				joinby obs`t' using temp_xchain_`t', unmatched(master)
				noisily tab _merge
				drop if _merge==2
				rename _merge _m`s'`t' 
				rename obs`t' new`s'
			}
		}
		gen _mjunk=0
		egen idx = rowmax(_m*)
		noisily tab idx
		keep if idx==3
		sort obs
		drop _m*
		save temp2_xchain_`s', replace
	}
*}

*/
**3 Assign single setyear to all members of a family
**
**put the above chains, each of which starts with a different year from 1989 to 2009, back together into 
**one file for the whole sample period; 
**challenge here is to set a single setyr for all "families" revealed by the chain; 
**note that there are two cases for a "family". in the first case, all members sprout from the same obsolete 
**code in some year. in the second, two sub-families in an early year are joined by a common code of set of codes 
**in a subsequent year. 
**the iteration of min commands below takes care of both cases by searching for the setyr for a family that
**covers all of its members.

use temp_xchain_2012, clear
/* 
THIS SECTION IS COMMENTED OUT DUE TO THERE ONLY BEING ONE CHANGE

forvalues y=`b1'/`e' {
	append using temp2_xchain_`y'
}
*/
keep obs new* setyr* effyr*
capture duplicates drop
egen double setyr = rowmin(setyr*)
egen nchain = rownonmiss(new*)
rename obs obsolete
order obs setyr
sort obs
save temp2_xchain, replace

use temp2_xchain, clear
drop setyr effyr*
egen t1 = seq(), by(obs)
reshape long new setyr, i(obs t1) j(effyr)
drop if new==. & setyr==.
drop t1 nchain
duplicates drop obs effyr new setyr, force
egen osd=sd(setyr), by(obs)
egen nsd=sd(setyr), by(new)
sum nsd osd
drop osd nsd 


*Now add back in the obsolete-new observations that are not part of chains.
*Have to add these in before the min loop below in case a non-chain obs-pair is part of a family
sort obsolete new effyr
merge 1:1 obsolete new effyr using hs6concordances2007_2012.dta
drop if effyr<`b' | effyr>`e'
tab _merge
drop _merge


*now start family identification loop
egen double t1     = min(setyr), by(obs)
rename setyr oldsetyr
local zzz = 2
local stop = 0
while `stop'==0 {
  quietly {
	noisily display [`zzz']
	local zlag = `zzz'-1
	if mod(`zzz',2)==0 { 	
		egen double t`zzz' = min(t`zlag'), by(new)
	}
	if mod(`zzz',2)~=0 {
		egen double t`zzz' = min(t`zlag'), by(obs)
	}
	compare t`zzz' t`zlag'
	gen idx = t`zzz'==t`zlag'
	tab idx
	local stop = r(r)==1
	local zzz = `zzz'+1
	display r(r) " " [`stop']
	drop idx
  }
}
local yyy = `zzz'-1
gen double setyr = t`yyy'
keep obs effyr new setyr
duplicates drop
sort obsolete new effyr
save  hs6concordancesFinal2007_2012.dta, replace
