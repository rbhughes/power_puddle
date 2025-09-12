notes on core_eia923\_\_monthly_generation_fuel

mmbtu = million British thermal units (BTU), a measure of energy content/heat value in fuel

Practical Example
If a power plant consumed 500 MMBtu of natural gas total, but 100 MMBtu was used for process steam:

```
                fuel_consumed_mmbtu = 500 MMBtu (total)
fuel_consumed_for_electricity_mmbtu = 400 MMBtu (electricity only)
```

This separation exists because some power plants are Combined Heat and Power (CHP\*) facilities that use fuel for both electricity generation and process steam production. The fuel_consumed_for_electricity_mmbtu field isolates only the portion used for electrical generation.

This distinction is crucial for calculating accurate heat rates, fuel costs per MWh, and emissions specifically attributable to electricity generation versus other industrial processes.

\* CHP details
The Fundamental Logic
CHP facilities don't use 100% of fuel for power generation because thermal energy has real economic value in industrial processes. By capturing and selling both electricity and heat, these facilities achieve:

Higher fuel utilization rates (up to 95% vs. 36% for electricity-only plants)

Lower emissions per unit of energy delivered

Better economics through dual revenue streams

Enhanced energy security for facilities requiring both electricity and heat

The goal isn't to maximize electricity production—it's to maximize useful energy output per unit of fuel consumed, making CHP one of the most efficient approaches to meeting combined electrical and thermal energy needs.

When evaluating carbon usage, CHP plants look different depending on your perspective:

Per unit of electricity: May appear higher if you only count electricity output but include all plant emissions

Per unit of total energy: Significantly lower due to higher overall efficiency

System-wide impact: Much lower carbon footprint than separate electricity and heating systems

The EPA estimates that a typical 5 MW CHP system produces about 4,200 tons of annual CO₂ emissions compared to 8,300 tons from equivalent separate electricity and heating systems - nearly 50% fewer emissions for the same energy services.

For accurate carbon assessments, you must use proper allocation methodologies that fairly distribute emissions between the electricity and thermal outputs of CHP facilities.

^^^^^^^^^^^^^^^^^^^^^

Name |Value |
-----------------------------------+-----------------------+
report_date |2001-02-01 00:00:00.000|
plant_id_eia |3 |
energy_source_code |BIT |
fuel_type_code_pudl |coal |
fuel_type_code_agg |COL |
prime_mover_code |UNK |
fuel_consumed_units |325972.0 |
fuel_consumed_for_electricity_units|325972.0 |
fuel_mmbtu_per_unit |23.76 |
fuel_consumed_mmbtu |7744323.0 |
fuel_consumed_for_electricity_mmbtu|7744323.0 |
net_generation_mwh |797159.0 |
data_maturity |final |

How to interpret this single row:

The Plant: Power plant #3 burned coal (specifically bituminous coal, coded as "BIT") during February 2001 to generate electricity.

Fuel Consumption: The plant consumed 325,972 units of coal. Since this is coal, these units are likely tons. So the plant burned about 326,000 tons of coal that month.

Energy Content: Each ton of coal contained 23.76 MMBtu of energy (think of this as the "energy density" - how much potential energy is packed into each ton of coal).

Total Energy Input: Multiplying the fuel amount by its energy content gives 7.74 million MMBtu of total energy fed into the plant.

Electricity Output: All this coal burning produced 797,159 MWh of electricity - enough to power roughly 600,000 average homes for a month.

Key Insight: Notice that fuel_consumed_units equals fuel_consumed_for_electricity_units (both are 325,972). This means 100% of the fuel went toward electricity generation - this plant wasn't a Combined Heat and Power facility, so it didn't use any fuel for process steam or heating.

Efficiency: The plant converted about 7.74 million MMBtu of coal energy into 797,159 MWh of electricity, which represents roughly 35% efficiency - typical for coal plants of that era, with the remaining 65% lost as waste heat.

FUEL CONSUMPTION = fuel_consumed_units
fuel_consumed_for_electricity_units

ENERGY_CONTENT = fuel_mmbtu_per_unit

TOTAL_ENERGY_INPUT = fuel_consumed \* fuel_mmbtu_per_unit

ELECTRICITY_OUTPUT = net_generation_mwh

### 3,412 BTU = 1 kWh

Step 1: Convert to Same Units
Input: 7.74 million MMBtu = 7,740,000,000,000 BTU

Output: 797,159 MWh

To convert MWh to BTU:

797,159 MWh × 1,000 kWh/MWh × 3,412 BTU/kWh = 2,719,370,108,000 BTU

Step 2: Apply the Formula
Efficiency = (2,719,370,108,000 BTU ÷ 7,740,000,000,000 BTU) × 100%

Efficiency = 35.1%

Here's a CHP plant record
Name |Value |
-----------------------------------+-----------------------+
report_date |2001-06-01 00:00:00.000|
plant_id_eia |10017 |
energy_source_code |BIT |
fuel_type_code_pudl |coal |
fuel_type_code_agg |COL |
prime_mover_code |ST |
fuel_consumed_units |10852.0 |
fuel_consumed_for_electricity_units|2320.75 |
fuel_mmbtu_per_unit |26.4 |
fuel_consumed_mmbtu |286495.0 |
fuel_consumed_for_electricity_mmbtu|61269.0 |
net_generation_mwh |7961.171 |
data_maturity |final |

...and the efficiency calculation

Step 1: Convert Electricity Output to Energy Units
Electricity output: 7,961.2 MWh

Convert to MMBtu: 7,961.2 MWh × 3.412 MMBtu/MWh = 27,163.5 MMBtu

Step 2: Apply the Formula
Fuel input for electricity: 61,269 MMBtu

Energy output: 27,163.5 MMBtu

Efficiency = (27,163.5 ÷ 61,269) × 100% = 44.3%
