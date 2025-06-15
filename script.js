// ✅ script.js (module-compatible with full function support)
import { createClient } from "https://cdn.jsdelivr.net/npm/@supabase/supabase-js/+esm";

const supabaseClient = createClient(
  "https://wzgchcvyzskespcfrjvi.supabase.co",
  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Ind6Z2NoY3Z5enNrZXNwY2ZyanZpIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDE4NjQwNDEsImV4cCI6MjA1NzQ0MDA0MX0.UuAgu4quD9Vg80tOUSkfGJ4doOT0CUFEUeoHsiyeNZQ"
);
localStorage.removeItem("user_id"); // ✅ run IMMEDIATELY on script load

window.dishNames = [];

window.showSection = function (sectionId) {
  console.log(`Switching to Section: ${sectionId}`);

  // Hide all sections
  document.querySelectorAll("section, div[id^='utility-']").forEach((el) => {
    el.style.display = "none";
  });

  // Show main utility section if needed
  if (sectionId.startsWith("utility-")) {
    const utilitiesSection = document.getElementById("utilities");
    if (utilitiesSection) {
      utilitiesSection.style.display = "block";
    }
  }

  // Show the requested section
  const targetSection = document.getElementById(sectionId);
  if (targetSection) {
    targetSection.style.display = "block";
  } else {
    console.error(`Section '${sectionId}' not found`);
  }
};

// Show utility sub-section
window.showUtilitySubSection = function (subSectionId) {
  document.querySelectorAll("div[id^='utility-']").forEach(el => el.style.display = "none");
  const target = document.getElementById(subSectionId);
  if (target) target.style.display = "block";
};

let dishNames = [];

window.loadDishNames = async function () {
  const { data, error } = await supabaseClient.from("food_items").select("dish_name");
  if (!error && data) {
    dishNames = data.map(d => d.dish_name);
  } else {
    console.error("Failed to load dish names:", error);
  }
};

window.addEventListener("DOMContentLoaded", () => {
  loadDishNames(); // make sure this is called on page load
});



window.setupAutocomplete = function (input) {
  // Wrap input in a relative container
  const wrapper = document.createElement("div");
  wrapper.style.position = "relative";
  input.parentNode.insertBefore(wrapper, input);
  wrapper.appendChild(input);

  // Create dropdown box
  const box = document.createElement("div");
  box.className = "autocomplete-box";
  box.style.position = "absolute";
  box.style.top = input.offsetHeight + "px";
  box.style.left = "0";
  box.style.right = "0";
  box.style.zIndex = "9999";
  box.style.background = "white";
  box.style.border = "1px solid #ccc";
  box.style.borderTop = "none";
  box.style.maxHeight = "150px";
  box.style.overflowY = "auto";
  box.style.display = "none";

  wrapper.appendChild(box);

  input.addEventListener("input", function () {
    const val = input.value.trim().toLowerCase();
    box.innerHTML = "";

    if (!val) {
      box.style.display = "none";
      return;
    }

    const matches = dishNames.filter(name =>
      name.toLowerCase().includes(val)
    ).slice(0, 10);

    if (matches.length === 0) {
      box.style.display = "none";
      return;
    }

    matches.forEach(match => {
      const div = document.createElement("div");
      div.textContent = match;
      div.style.padding = "6px";
      div.style.cursor = "pointer";
      div.style.borderBottom = "1px solid #eee";
      div.addEventListener("click", () => {
        input.value = match;
        box.innerHTML = "";
        box.style.display = "none";
      });
      box.appendChild(div);
    });

    box.style.display = "block";
  });

  // Hide when clicking outside
  document.addEventListener("click", function (e) {
    if (!wrapper.contains(e.target)) {
      box.style.display = "none";
    }
  });
};



// ✅ Add a dish row
window.addDishRow = function (mealType, name = "", grams = "") {
  const container = document.getElementById(`${mealType}-container`);
  const row = document.createElement("div");
  row.className = "dish-row";

  row.innerHTML = `
    <input type="text" class="dish-name responsive-dish-name" value="${name}" placeholder="Dish Name" />
    <input type="number" class="dish-grams responsive-dish-grams" value="${grams}" placeholder="gms" />
    <button type="button" class="remove-btn" onclick="this.parentElement.remove()">✖</button>
  `;

  container.appendChild(row);

  const input = row.querySelector(".dish-name");
  setupAutocomplete(input); // autocomplete support
};




// ✅ Fetch dish info
window.getDishInfo = async function (name) {
  const { data, error } = await supabaseClient
    .from("food_items")
    .select("*")
    .ilike("dish_name", name.trim());

  if (!error && data && data.length) return data[0];
  return null;
};


// ✅ Calculate total macros
window.calculateCalories = async function () {
  const meals = ["breakfast", "lunch", "dinner"];
  let totals = { calories: 0, protein: 0, carbs: 0, fibre: 0, fats: 0 };
  let today = new Date().toISOString().split("T")[0];
  let dishEntries = [];

  for (const meal of meals) {
    const container = document.getElementById(`${meal}-container`);
    const rows = container.querySelectorAll(".dish-row");

    for (const row of rows) {
      const name = row.querySelector(".dish-name").value;
      const grams = parseFloat(row.querySelector(".dish-grams").value);
      if (!name || isNaN(grams)) continue;

      const info = await window.getDishInfo(name);
      if (!info) continue;

      const factor = grams / 100;
      totals.calories += (info.calorie_per_100gm || 0) * factor;
      totals.protein += (info.protein_per_100gm || 0) * factor;
      totals.carbs += (info.carbs_per_100gm || 0) * factor;
      totals.fibre += (info.fibre_per_100gm || 0) * factor;
      totals.fats += (info.fats_per_100gm || 0) * factor;

      dishEntries.push({
        date: today,
        meal_type: meal,
        dish_name: name,
        quantity_grams: grams
      });
    }
  }

  // Show calculated totals
  document.getElementById("calorie-result").innerHTML = `
    <strong>Total:</strong><br>
    Calories: ${totals.calories.toFixed(2)} kcal<br>
    Protein: ${totals.protein.toFixed(2)} g<br>
    Carbs: ${totals.carbs.toFixed(2)} g<br>
    Fibre: ${totals.fibre.toFixed(2)} g<br>
    Fats: ${totals.fats.toFixed(2)} g`;

  // Save to DB and show summary
  await window.saveDishRowsToDB(dishEntries);

  const summaryContainer = document.getElementById("summary-container");
  summaryContainer.style.display = "block";
  await window.loadDishSummaryTable();

  // Scroll to summary smoothly
  summaryContainer.scrollIntoView({ behavior: "smooth" });
};



// ✅ Save dish entries to DB
window.saveDishRowsToDB = async function (dishEntries) {
  const today = new Date().toISOString().split("T")[0];
  const userId = localStorage.getItem("user_id");

  if (!userId) {
    console.error("❌ No user_id found in localStorage");
    return;
  }

  // Delete existing dishes for this user and date
  await supabaseClient
    .from("daily_dishes")
    .delete()
    .eq("date", today)
    .eq("user_id", userId);

  const rowsToInsert = [];

  for (const entry of dishEntries) {
    const info = await window.getDishInfo(entry.dish_name);
    if (!info) continue;

    const factor = entry.quantity_grams / 100;
    rowsToInsert.push({
      user_id: userId,
      date: today,
      meal_type: entry.meal_type,
      dish_name: entry.dish_name,
      grams: entry.quantity_grams,
      calories: (info.calorie_per_100gm || 0) * factor,
      protein: (info.protein_per_100gm || 0) * factor,
      carbs: (info.carbs_per_100gm || 0) * factor,
      fibre: (info.fibre_per_100gm || 0) * factor,
      fats: (info.fats_per_100gm || 0) * factor
    });
  }

  if (rowsToInsert.length) {
    await supabaseClient.from("daily_dishes").insert(rowsToInsert);
  }
};


// Load summary table
window.loadDishSummaryTable = async function () {
  const today = new Date().toISOString().split("T")[0];
  const userId = localStorage.getItem("user_id");

  if (!userId) {
    console.error("❌ No user_id found in localStorage");
    return;
  }

  const { data: dishes, error } = await supabaseClient
    .from("daily_dishes")
    .select("*")
    .eq("date", today)
    .eq("user_id", userId);

  if (error) {
    console.error("❌ Error fetching dish summary:", error.message);
    return;
  }

  const tbody = document.getElementById("dish-summary-body");
  tbody.innerHTML = "";

  let totalCalories = 0, totalProtein = 0, totalCarbs = 0, totalFibre = 0, totalFats = 0;

  dishes.forEach(dish => {
    const row = document.createElement("tr");
    row.innerHTML = `
      <td>${dish.dish_name}</td>
      <td>${dish.grams.toFixed(1)}</td>
      <td>${dish.calories.toFixed(1)}</td>
      <td>${dish.protein.toFixed(1)}</td>
      <td>${dish.carbs.toFixed(1)}</td>
      <td>${dish.fibre.toFixed(1)}</td>
      <td>${dish.fats.toFixed(1)}</td>
    `;
    tbody.appendChild(row);

    totalCalories += dish.calories || 0;
    totalProtein += dish.protein || 0;
    totalCarbs += dish.carbs || 0;
    totalFibre += dish.fibre || 0;
    totalFats += dish.fats || 0;
  });

  const totalRow = document.createElement("tr");
  totalRow.style.backgroundColor = "#f0f0f0";
  totalRow.style.fontWeight = "bold";
  totalRow.innerHTML = `
    <td colspan="2">Total</td>
    <td>${totalCalories.toFixed(1)}</td>
    <td>${totalProtein.toFixed(1)}</td>
    <td>${totalCarbs.toFixed(1)}</td>
    <td>${totalFibre.toFixed(1)}</td>
    <td>${totalFats.toFixed(1)}</td>
  `;
  tbody.appendChild(totalRow);
};


// Login handlers
window.promptCalorieLogin = async function () {
  const userId = localStorage.getItem("user_id");

  if (userId && userId.trim() !== "") {
    // 🔍 Verify this user actually exists in Supabase
    const { data, error } = await supabaseClient
      .from('app_users')
      .select('username')
      .eq('username', userId)
      .maybeSingle();

    if (!error && data) {
      console.log("✅ Valid user found:", userId);
      window.showSection('utility-daily-calorie');
      window.loadDishSummaryTable();
      return;
    } else {
      console.warn("⚠️ User ID in localStorage is stale. Clearing it.");
      localStorage.removeItem("user_id");  // clear stale ID
    }
  }

  console.log("🔐 No valid login — showing login modal");
  document.getElementById('loginModal').style.display = 'block';
};



window.handleCalorieLogin = async function () {
  const username = document.getElementById("usernameInput").value.trim().toLowerCase();
  const password = document.getElementById("passwordInput").value.trim();

  const { data, error } = await supabaseClient
    .from('app_users')
    .select('*')
    .ilike('username', username)  // case-insensitive username
    .eq('password', password)     // case-sensitive password
    .single();

  if (error || !data) {
    alert("Invalid username or password.");
    return;
  }

  // ✅ Only runs if data is present
  localStorage.setItem("user_id", data.username);
  document.getElementById("loginModal").style.display = "none";
  window.showSection('utility-daily-calorie');
  window.loadDishSummaryTable();
  await window.loadDailyDishes();
};



// Load food facts
window.loadFoodFacts = async function () {
  const { data, error } = await supabaseClient.from("food_items").select("*");
  const tbody = document.querySelector("#food-facts-table tbody");
  tbody.innerHTML = "";

  if (!error && data) {
    data.forEach(dish => {
      const row = document.createElement("tr");
      row.innerHTML = `
        <td>${dish.dish_name}</td>
        <td>${dish.calorie_per_100gm || 0}</td>
        <td>${dish.protein_per_100gm || 0}</td>
        <td>${dish.carbs_per_100gm || 0}</td>
        <td>${dish.fibre_per_100gm || 0}</td>
        <td>${dish.fats_per_100gm || 0}</td>`;
      tbody.appendChild(row);
    });
    enableTableSorting();  // <-- important call added here
  }
};


// Show 'utilities' section on load
document.addEventListener("DOMContentLoaded", () => {
  window.showSection("utilities");
});
document.addEventListener("DOMContentLoaded", () => {
  const form = document.getElementById("nutrition-form");
  if (form) {
    form.addEventListener("submit", async (e) => {
      e.preventDefault();

      const dishName = document.getElementById("dish_name").value.trim();
      const calorie = parseFloat(document.getElementById("calorie").value);
      const protein = parseFloat(document.getElementById("protein").value);
      const carbs = parseFloat(document.getElementById("carbs").value);
      const fibre = parseFloat(document.getElementById("fibre").value);
      const fats = parseFloat(document.getElementById("fats").value);

      const { error } = await supabaseClient.from("food_items").insert([
        {
          dish_name: dishName,
          calorie_per_100gm: calorie,
          protein_per_100gm: protein,
          carbs_per_100gm: carbs,
          fibre_per_100gm: fibre,
          fats_per_100gm: fats
        }
      ]);

      const message = document.getElementById("nutrition-message");
      if (error) {
        message.textContent = "❌ Failed to save dish.";
        message.style.color = "red";
      } else {
        message.textContent = "✅ Dish saved successfully.";
        message.style.color = "green";
        form.reset();
      }
    });
  }
});

function enableTableSorting() {
  const table = document.getElementById("food-facts-table");
  const headers = table.querySelectorAll("th");
  const tbody = table.querySelector("tbody");

  headers.forEach((header, index) => {
    header.style.cursor = "pointer";
    header.addEventListener("click", () => {
      const isAsc = header.classList.contains("asc");
      const rows = Array.from(tbody.querySelectorAll("tr"));
      rows.sort((a, b) => {
        const aVal = parseFloat(a.children[index].textContent) || a.children[index].textContent.toLowerCase();
        const bVal = parseFloat(b.children[index].textContent) || b.children[index].textContent.toLowerCase();
        return (isAsc ? (bVal > aVal ? 1 : -1) : (aVal > bVal ? 1 : -1));
      });
      headers.forEach(h => h.classList.remove("asc", "desc"));
      header.classList.add(isAsc ? "desc" : "asc");
      rows.forEach(row => tbody.appendChild(row));
    });
  });
}

document.addEventListener("DOMContentLoaded", () => {
  const btn = document.getElementById("calculate-btn");
  if (btn) {
    btn.addEventListener("click", window.calculateCalories);
  }
});
window.loadDailyDishes = async function () {
  const today = new Date().toISOString().split("T")[0];
  const userId = localStorage.getItem("user_id");

  if (!userId) {
    console.error("❌ No user_id found in localStorage");
    return;
  }

  const { data, error } = await supabaseClient
    .from("daily_dishes")
    .select("*")
    .eq("date", today)
    .eq("user_id", userId);

  if (error) {
    console.error("Error loading daily dishes:", error);
    return;
  }

  const meals = ["breakfast", "lunch", "dinner"];
  meals.forEach(meal => {
    const container = document.getElementById(`${meal}-container`);
    container.innerHTML = ""; // Clear old rows

    data
      .filter(d => d.meal_type === meal)
      .forEach(dish => {
        window.addDishRow(meal, dish.dish_name, dish.grams);
      });
  });
};

let deferredPrompt;
const installBtn = document.getElementById('installApp');

// Listen for the install prompt event
window.addEventListener('beforeinstallprompt', (e) => {
  e.preventDefault(); // Prevent auto-prompt
  deferredPrompt = e;
  installBtn.style.display = 'inline-block'; // Show the button
});

// Handle click on install button
installBtn.addEventListener('click', () => {
  if (deferredPrompt) {
    deferredPrompt.prompt();
    deferredPrompt.userChoice.then(choiceResult => {
      console.log('User response to the install prompt:', choiceResult.outcome);
      deferredPrompt = null;
      installBtn.style.display = 'none';
    });
  }
});

if ('serviceWorker' in navigator) {
  navigator.serviceWorker.register('service-worker.js').then(registration => {
    console.log('✅ SW registered');

    registration.onupdatefound = () => {
      const newSW = registration.installing;
      newSW.onstatechange = () => {
        if (newSW.state === 'installed') {
          if (navigator.serviceWorker.controller) {
            console.log('🔄 New version available — reloading...');
            window.location.reload(); // Reload to activate the new SW
          }
        }
      };
    };
  });
}

