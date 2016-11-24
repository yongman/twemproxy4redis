
primary = {
   tc = {"$master"},
   jx = {"$master"},
   nj02 = {"$master"},
   nj03 = {"$master"},
   hz01 = {"$master"},
}

primary_preferred = {
   tc = {"$master","tc","jx",{"nj","nj03","hz"}},
   jx = {"$master","jx","tc",{"nj","nj03","hz"}},
   nj = {"$master","nj","nj03","hz",{"tc","jx"}},
   nj03 = {"$master","nj03","nj","hz",{"tc","jx"}},
   hz = {"$master","hz",{"nj","nj03"},{"tc","jx"}}
}

nearest = {
   tc = {"tc","jx",{"nj","nj03","hz","sh"},"gz"},
   jx = {"jx","tc",{"nj","nj03","hz","sh"},"gz"},
   nj = {"nj",{"nj03","hz","sh"},{"tc","jx"},"gz"},
   nj03 = {"nj03",{"nj","hz","sh"},{"tc","jx"},"gz"},
   hz = {"hz",{"nj","nj03","sh"},{"tc","jx"},"gz"},
   sh = {"sh",{"nj","nj03","hz"},"gz",{"tc","jx"}},
   gz = {"gz", {"nj","nj03","hz"},"sh",{"tc","jx"}}
}

return nearest
