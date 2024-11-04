* Set the seed for reproducibility
set seed 123456

// * Define the sample size
// set obs 1000

// * Generate the running variable 'size' (house size in m²)
// gen size = runiform(50, 130)

* Create an indicator for the treatment (size >= 90)
gen D = (area >= 90)

* Create the binary outcome variable 'Y' with a baseline probability of 0.1
drop if area==-8


// * Increase the probability of Y for observations where size >= 90
// replace Y = (runiform() < 0.2) if D == 1

* Summarize the data to check the basic statistics
summarize area divorce D

* Restrict the sample to the bandwidth (80 <= size <= 100)
// keep if size >= 80 & size <= 100

* Check the distribution of the outcome variable around the cutoff
// table D, statistic(mean Y) statistic(count Y)

* Visualize the data to see the discontinuity
twoway (scatter divorce area, msymbol(o) mcolor(blue)) ///
       (line D area, sort lcolor(red) lwidth(medium)) ///
       , title("Scatter plot with discontinuity at 90 m²") ///
       xline(90, lpattern(dash)) ///
       ylabel(0 1) ///
       xlabel(80(2)100)

* Use the rdrobust command to perform the regression discontinuity analysis
rdrobust divorce area, c(90) h(10)  // Bandwidth of 10 units on each side of the cutoff
// reg divorce D if area >= 80 & area <= 100



reg divorce D 
